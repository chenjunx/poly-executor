# poly-executor

Polymarket 预测市场自动化交易执行器，支持**配对套利**和**流动性奖励做市**两种策略，通过 WebSocket 实时订阅行情，订单与行情数据持久化到 SQLite。

---

## 目录

- [功能概览](#功能概览)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [CSV 文件格式](#csv-文件格式)
- [数据库表结构](#数据库表结构)
- [模块说明](#模块说明)

---

## 功能概览

| 功能 | 说明 |
|---|---|
| 配对套利（PairArbitrage） | 监听 YES/NO token 对，当 `1 - (ask_YES + ask_NO) > min_diff` 时发出套利信号 |
| 流动性奖励做市（LiquidityReward） | 围绕 mid 价持续挂 limit 单，赚取 Polymarket 官方做市奖励 |
| 模拟模式 | 全局或策略级模拟开关，本地模拟成交与仓位，不发送真实订单 |
| 故障恢复 | 启动时从 SQLite 自动恢复订单关联关系和做市挂单状态 |
| 行情录制 | 可选将 best tick / 全量订单簿快照 / 成交事件异步写入 SQLite |
| 奖励监控 | 实时估算做市奖励得分并落盘；定期轮询 Polymarket 奖励 API |
| 代理支持 | 支持 SOCKS5 / HTTP 代理连接 WebSocket |

---

## 快速开始

1. 复制并填写配置文件：

```bash
cp config.toml config.local.toml
# 编辑 config.local.toml，填写 auth / order 等敏感配置
```

2. 准备 CSV 文件（参见 [CSV 文件格式](#csv-文件格式)）：

```
assets.csv              # 配对套利的 token 对
liquidity_reward.csv    # 做市规则
```

3. 编译并运行：

```bash
cargo build --release
./target/release/poly-executor
```

> 配置文件查找优先级：先读取可执行文件同目录的 `config.toml`，再用 `config.local.toml` 叠加覆盖（`config.local.toml` 建议加入 `.gitignore`，存放密钥等敏感配置）。

---

## 配置说明

### `[proxy]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `url` | String | `""` | 代理地址，支持 `socks5://host:port` 或 `http://host:port`；为空时自动读取 `ALL_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` 环境变量 |

---

### `[auth]`

| 配置项 | 类型 | 说明 |
|---|---|---|
| `api_key` | String | Polymarket CLOB API Key |
| `api_secret` | String | Polymarket CLOB API Secret |
| `passphrase` | String | Polymarket CLOB API Passphrase |
| `private_key` | String | EIP-712 签名私钥（`0x` 开头），用于链上订单签名 |
| `funder` | String | 钱包地址 |

---

### `[order]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | `true` 才启动真实订单链路（含订单 WebSocket 监听）；`false` 时即使 `simulation=false` 也不下单 |
| `size_usdc` | f64 | `10.0` | 配对套利每次投入的 USDC 总量 |

---

### `[simulation]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 全局模拟模式；`true` 时跳过真实订单 WebSocket 和持仓 API，改为本地模拟成交与仓位 |

---

### `[liquidity_reward]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 是否启用流动性奖励做市策略 |
| `file` | String | `"liquidity_reward.csv"` | 做市规则 CSV 文件路径 |
| `monitor_enabled` | bool | `false` | 是否启动奖励监控轮询任务（非模拟模式下生效） |
| `simulation` | bool | `false` | 做市策略内部模拟开关，独立于全局 `[simulation]`；模拟模式下奖励得分写入时会打 `simulation=1` 标记 |

> 注：`[liquidity_reward]` 在代码中兼容历史别名 `[mid_requote]`。

---

### `[app]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `log_file` | String | `"alerts.log"` | 告警/策略日志文件名 |
| `order_log_file` | String | `"orders.log"` | 订单专用日志文件名 |
| `assets_file` | String | `"assets.csv"` | 配对套利 token 对配置文件路径 |
| `sqlite_path` | String | `"orders.db"` | SQLite 数据库路径（绝对路径或相对可执行文件目录） |
| `min_diff` | f64 | — | 套利触发阈值：`1 - (ask_YES + ask_NO) > min_diff` |
| `max_spread` | f64 | — | 单 token 最大 bid/ask 价差上限，超过则跳过 |
| `min_price` | f64 | — | 有效价格区间下限，低于此值跳过 |
| `max_price` | f64 | — | 有效价格区间上限，超过此值跳过 |
| `default_threads` | usize | — | 未在 `[topic_threads]` 中单独配置的 topic 使用的 WebSocket 连接数 |
| `monitor_interval_secs` | u64 | `30` | 做市奖励 API 轮询间隔（秒） |
| `tick_store_enabled` | bool | `false` | 是否将 best bid/ask 变化时的 tick 写入 `market_ticks` 表 |
| `raw_store_enabled` | bool | `false` | 是否将全量订单簿快照写入 `book_snapshots` 表、成交事件写入 `trade_events` 表 |

---

### `[topic_threads]`

为特定 topic 分配独立的 WebSocket 连接数，未列出的 topic 使用 `default_threads`。

```toml
[topic_threads]
us_iran = 1
# cs = 2
# sports = 4
```

---

## CSV 文件格式

### `assets.csv`（配对套利）

每行定义一对需要监控套利机会的 token，**含表头行**：

| 列 | 字段名 | 类型 | 说明 |
|---|---|---|---|
| 0 | `token0` | String | YES token 的 Polymarket asset ID |
| 1 | `token1` | String | 配对 token 的 asset ID（同一市场对立 outcome） |
| 2 | `topic` | String | 订阅分组名，对应 `[topic_threads]` 中的键；同一 topic 的 token 共享 WebSocket 连接 |

示例：

```csv
token0,token1,topic
84133519426074676...,53265937461843025...,us_iran
```

---

### `liquidity_reward.csv`（做市奖励规则）

每行定义一个市场的做市规则，**含表头行**：

| 列 | 字段名 | 类型 | 必填 | 说明 |
|---|---|---|---|---|
| 0 | `token1` | String | 是 | YES token asset ID，策略对此 token 挂双边 limit 单 |
| 1 | `token2` | String | 否 | 配对 NO token asset ID；为空时仅对 token1 单边做市 |
| 2 | `topic` | String | 否 | 订阅分组名；为空时默认使用 `"liquidity_reward"` |
| 3 | `reward_min_orders` | u32 | 否 | 奖励达标所需的最少挂单数量（用于 monitor 评估） |
| 4 | `reward_max_spread_cents` | f64 | 否 | 奖励有效的最大价差（单位：cents，如 `4` 表示 4 cents） |
| 5 | `reward_min_size` | f64 | 否 | 奖励有效的最小单笔挂单金额（USDC） |
| 6 | `reward_daily_pool` | f64 | 否 | 该市场每日奖励总池（USDC），用于估算预期日收益份额 |

示例：

```csv
token1,token2,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool
84133519426074676...,53265937461843025...,us_iran,5,4,100,50
```

---

## 数据库表结构

| 表名 | 写入时机 | 说明 |
|---|---|---|
| `orders` | 每次下单/状态变更 | 核心订单表；`status` 值：`pending` / `open` / `filled` / `canceled` / `rejected` |
| `order_events` | 订单生命周期事件 | 订单流水，`payload_json` 存储原始事件 JSON |
| `strategy_state_mid_requote` | 做市策略状态变更 | 做市策略共享状态快照，用于崩溃恢复 |
| `strategy_state_mid_requote_side` | 做市策略状态变更 | 做市策略双侧（buy/sell）状态快照，PK 为 `(token, side)` |
| `liquidity_reward_scores` | 奖励估算周期 | 做市奖励得分记录；包含 `my_qmin`、预计份额、估算日收益等字段 |
| `market_ticks` | `tick_store_enabled=true` | best bid/ask 变化时的 tick 记录；price/size 精度为 1/10000 的整数 |
| `book_snapshots` | `raw_store_enabled=true` | 全量订单簿快照；`bids`/`asks` 为 BLOB，格式：每档 6 字节 = `price(u16 LE)` + `size(u32 LE)` |
| `trade_events` | `raw_store_enabled=true` | `last_trade_price` 成交事件；字段：`token`、`market`、`price`、`side`、`size`、`fee_rate`、`ts_ms` |

`book_snapshots` BLOB 解析（Python 示例）：

```python
import struct

def parse_levels(blob: bytes) -> list[tuple[float, float]]:
    levels = []
    for i in range(0, len(blob), 6):
        price = struct.unpack_from('<H', blob, i)[0] / 10000
        size  = struct.unpack_from('<I', blob, i + 2)[0] / 10000
        levels.append((price, size))
    return levels
```

---

## 模块说明

| 模块 | 说明 |
|---|---|
| `market.rs` | WebSocket 行情订阅；维护每个 token 的本地 BTreeMap 订单簿；驱动行情事件分发 |
| `dispatcher.rs` | 将行情事件按 topic/token 路由到各策略 channel |
| `order.rs` | 接收策略的 `OrderSignal`，真实或模拟下单，写订单日志和 SQLite |
| `order_ws.rs` | 连接 Polymarket 私有订单频道，实时回写订单状态 |
| `positions.rs` | 真实模式拉取持仓；模拟模式维护内存仓位 |
| `monitor.rs` | 实时估算做市奖励得分；定期轮询 Polymarket 奖励 API |
| `recovery.rs` | 启动时从 SQLite 恢复订单关联关系和做市挂单状态 |
| `storage.rs` | SQLite 封装；WAL + NORMAL sync 模式 |
| `proxy_ws.rs` | SOCKS5 / HTTP 代理 WebSocket 连接层 |
| `polymarket_rewards.rs` | Polymarket 官方 Q-score 做市奖励积分计算算法 |
