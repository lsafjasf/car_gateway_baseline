# 车载采集网关（Python 3.12，仅标准库）

多采集线程 → 本地 flash 持久化 → 网络可用时批量上云。设备可随时拔电：
**fsync 返回后的数据不丢、不产生半条脏记录、未收到云端确认绝不回收空间。**

## 快速开始

```bash
# 终端 1：mock 云端（也可换成真实云，协议见下）
python -m gateway.cloud_mock --state-dir ./_run/cloud --port 8080

# 终端 2：网关
python -m gateway.app --config config.example.json

# 看积压
curl http://127.0.0.1:8081/stats

# 端到端演示（自启 mock 云 + 3 个模拟采集线程，跑约 5 秒）
python -m examples.demo
```

测试：

```bash
python -m pytest          # 43 个用例，覆盖断电恢复/ACK 丢失/配额阻塞/多线程 E2E
```

## 工作机制

### 1. 写入：单写线程 + 组提交 + 每次提交 fsync

- 多个采集线程调用 `Gateway.submit(dict) -> seq`；帧被序列化后**按字节预留
  flash 配额**，再进入有界内存队列。
- 唯一写线程把等待中的帧攒成一组（默认 ≤256 条或 ≤10ms），一次
  `write + flush + os.fsync`，返回前逐帧 `Event` 唤醒采集线程。
  **`submit()` 返回 = 该帧已经 fsync**，这是“已写完”的唯一定义。
- 记录为二进制模式追加的 JSONL（`ab`，不做文本模式换行翻译）：
  `{"seq":N,"ts":...,"payload":<用户对象>}\n`，一行一条。
- 全局 `seq` 稠密递增，由写线程在出队时分配（不相信时间戳排序）。
- 段文件 `segment-000001.log …`（默认 8MB 或 2s 轮转，**轮转只发生在两个
  组提交之间**；空闲时写线程也会按年龄轮转，保证尾部帧不滞留）。

### 2. 断电恢复

启动时 `Store.open()` 单线程扫描所有段：

- 每个**非最高位段**必须是 seq 稠密的合法记录链；任何坏行、段间 seq 缺口
  → `DataCorruptionError` 拒绝启动（隔离目录人工处理，绝不静默截断）。
- 最高位段允许有**撕裂尾**（写一半掉电）：逐行校验到第一条坏行（含没有
  `\n` 的尾行、JSON 解析失败、段内 seq 跳跃），`truncate` 到最后一条
  完整记录之后并 fsync。
- `state.json` 为水位 `{acked_seq, crc32}`，tmp + `os.replace` + 目录
  fsync 原子写；解析失败 / CRC 错 / acked 超过恢复出的最大 seq → **向安全
  方向**水位归 0 并告警（代价只是重传，绝不误删数据）。

### 3. 上云、ACK 与空间回收（at-least-once）

- 上传线程只读取**已关闭、不可变**的段，按 seq 顺序组批
  （默认 500 条 / 256KB），`http.client` POST 到 `/batches`：
  `{"batch_id": "b000…-…", "first_seq": N, "frames": [...]}`。
- 同一批失败/超时/响应丢失后，**原样重发完全相同的 batch_id 与报文字节**，
  指数退避 0.5s→30s；网络断开时数据持续积压在 flash。
- 云端契约（mock 云即参考实现）：只维护一个连续水位，`seq ≤ watermark`
  的帧视为重复直接忽略；新帧必须连续，缺口回 409；**新水位先 fsync
  落盘再回 ACK**。因此 ACK 丢失/任一方重启都不会重复入库。
- 回收顺序：收到 ACK → **先**原子持久化 `state.json` → **再** `unlink`
  已关闭且 `max_seq ≤ acked_seq` 的整段 → fsync 目录。活动段、部分确认段
  一律保留。未确认数据在任何崩溃窗口中都不会被回收。

### 4. flash 配额（默认 512MB，可配）

- 闸门是 `现存段字节 + 已预留字节 ≤ quota_bytes`；写满且云端未确认时
  **采集线程阻塞在 `submit()` 里**（不丢数），ACK 推进、段被删除后
  `notify_all` 唤醒。

### 5. 本地 HTTP 接口（标准库 ThreadingHTTPServer）

`GET /stats`：

```json
{
  "backlog": 1234,
  "bytes_used": 98231,
  "quota_bytes": 536870912,
  "reclaimable_bytes": 0,
  "oldest_ts": 1789012345.678,
  "max_seq": 5678,
  "acked_seq": 4444
}
```

`GET /healthz` → `{"ok": true}`。`oldest_ts` 借助每 128KB 一个采样点的
稀疏索引精确定位最早未确认帧；积压为 0 时为 `null`。

## 代码结构

```
gateway/
  config.py     全部可调参数（Config dataclass，JSON 加载）
  models.py     Stats/Sample/异常类型/Clock 协议
  durability.py fsync_file / fsync_dir(Windows no-op) / atomic_replace_write
  segments.py   存储核心：恢复截断、配额预留、整段回收、稀疏索引、批次读取
  writer.py     写线程：组提交、seq 分配、配额阻塞、空闲轮转、排空停机
  uploader.py   上传线程：不可变批次重发、指数退避、ACK 应用、停机 drain
  cloud_mock.py 参考云：水位去重 + 水位持久化 + fail_next/drop_next 故障注入
  api.py        GET /stats、/healthz
  app.py        Gateway 装配（submit/start/stop）与 python -m gateway.app
tests/          43 个 pytest 用例
examples/demo.py
```

## 关键设计取舍

- **只上传已关闭段**：上传侧无需协调活动段的并发读与“已落盘偏移”，回收判定
  也最简单；代价是尾部最多延迟一个段年龄（默认 2s，可调）。
- **水位而非已收集合**：单上传线程严格按序发送 + 云端连续水位，即可用一个
  整数完成全局去重；mock 云重启从 `watermark.json` 恢复水位。
- **配额用预留制**：阻塞精确发生在采集线程 `submit()` 内，写线程永远不会
  在一个组的中途因空间不足卡住。
- **状态异常一律向安全侧**：`state.json` 损坏只导致重传，不可能导致误删。
