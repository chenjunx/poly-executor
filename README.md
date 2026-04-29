# poly-executor

Polymarket 预测市场自动化交易执行器，支持**配对套利**和**流动性奖励做市**两种策略，通过 WebSocket 实时订阅行情，并将真实订单状态与行情/模拟分析数据分库存储到 SQLite。

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
| `size_usdc` | f64 | `10.0` | 配对套利每次投入的 USDC 总量 |

---

### `[chain]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `rpc_url` | String | `"https://polygon-rpc.com"` | Polygon 主网 RPC 地址，用于 `merge` 工具等链上操作；建议换成 Alchemy/Infura 专用节点提高稳定性 |

---

### `[simulation]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 全局模拟开关。`true` 时跳过所有与交易所的真实交互（真实订单 WebSocket、持仓 API、下单请求），改为本地模拟成交与仓位。**这是控制是否真实交易的唯一开关。** |

> 注：原 `[order] enabled` 字段已移除，统一由 `[simulation] enabled` 控制。

---

### `[liquidity_reward]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 是否启用流动性奖励做市策略 |
| `file` | String | `"liquidity_reward.csv"` | 做市规则 CSV 文件路径 |
| `monitor_enabled` | bool | `false` | 是否启动奖励监控轮询任务（非模拟模式下生效） |
| `reward_estimator_enabled` | bool | `true` | 是否启用本地 Q-score 奖励估算；开启后写入行情库 `liquidity_reward_scores` 表 |
| `simulation` | bool | `false` | 做市策略内部模拟开关，独立于全局 `[simulation]`；模拟模式下奖励得分写入时会打 `simulation=1` 标记 |

> 注：`[liquidity_reward]` 在代码中兼容历史别名 `[mid_requote]`。

---

### `[app]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `log_file` | String | `"alerts.log"` | 告警/策略日志文件名 |
| `order_log_file` | String | `"orders.log"` | 订单专用日志文件名 |
| `assets_file` | String | `"assets.csv"` | 配对套利 token 对配置文件路径 |
| `sqlite_path` | String | `"orders.db"` | 订单库路径，保存真实订单、订单事件和策略恢复状态 |
| `market_sqlite_path` | String | `""` | 行情/模拟库路径；为空时默认使用订单库同目录下的 `market.db` |
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

## 工具命令

### `merge` — 头寸合并（YES+NO → USDC）

将等量的 YES token 和 NO token 通过 Polymarket CTF 合约（`mergePositions`）换回 USDC。

```bash
cargo build --release --bin merge
./target/release/merge <condition_id> <amount_usdc>
```

| 参数 | 说明 |
|---|---|
| `condition_id` | 市场的 condition ID，`0x` 开头 64 位十六进制 |
| `amount_usdc` | 要合并的数量（USDC，如 `100.0`） |

读取 `config.toml` / `config.local.toml` 中的 `[auth] private_key` 和 `[chain] rpc_url`，直接向 Polygon 发起链上交易。

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
| 0 | `token1` | String | 是 | YES token asset ID；策略会对此 token 挂买单 |
| 1 | `token2` | String | 否 | 配对 NO token asset ID；填写后策略也会对此 token 挂买单；为空时仅对 token1 做市 |
| 2 | `topic` | String | 否 | 订阅分组名；为空时默认使用 `"liquidity_reward"` |
| 3 | `reward_min_orders` | u32 | 否 | 奖励达标所需的最少挂单数量（用于 monitor 评估） |
| 4 | `reward_max_spread_cents` | f64 | 否 | 奖励有效的最大价差（**单位：cents**，如 `4` 表示 4 cents；典型值 2–5）。低于此价差的挂单才计入 Q-score。填 `0.04` 表示 0.04 cents，极其严苛，几乎无竞争者得分，建议填整数如 `3` 或 `4`。 |
| 5 | `reward_min_size` | f64 | 否 | 奖励有效的最小单笔挂单金额（USDC） |
| 6 | `reward_daily_pool` | f64 | 否 | 该市场每日奖励总池（USDC），用于估算预期日收益份额 |

示例：

```csv
token1,token2,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool
84133519426074676...,53265937461843025...,us_iran,5,4,100,50
```

### LiquidityReward 报价与避开 best_bid 逻辑

策略对 `liquidity_reward.csv` 中配置的每个 token 独立维护一个买单状态。填写 `token1` 和 `token2` 时，会分别在 YES token 和 NO token 上挂买单，而不是在同一个 token 上同时挂买卖双边。

#### 实时价格输入

每次收到 market WebSocket 的 `book` 或 `price_change` 后，`market.rs` 会维护本地完整订单簿，并把最新 top-of-book 和全量 bids/asks 传给策略：

```text
best_bid = 当前订单簿最高 bid
best_ask = 当前订单簿最低 ask
mid      = (best_bid + best_ask) / 2
```

`mid` 是随行情事件实时更新的。策略是否撤单/重挂不再只看旧单是否低于 `mid - offset`，而是重新计算当前应该挂的目标价。

#### offset 与 tick

`reward_max_spread_cents` 的单位是 cents：

```text
offset = reward_max_spread_cents / 100
```

例如 `reward_max_spread_cents = 4` 表示 `offset = 0.04`。策略下单前会读取 token 当前 tick size，并按买单常用方式向下取合法价格，避免买单价格被四舍五入抬高。

#### 目标价与奖励有效区间

策略先计算两个价格：

```text
target_price     = mid - offset / 2
min_reward_price = mid - offset
```

含义：

- `target_price`：理想挂单价，位于奖励有效区间中间，更接近 mid。
- `min_reward_price`：奖励有效的最低买单价，低于它通常不再计入该配置的 Q-score 区间。

因此旧单只要价格和当前 `desired_price` 不一致，就会进入 replacement 流程；不会再因为仍位于 `[mid - offset, best_bid]` 区间内而继续保留。

#### 避免自己成为 best_bid

策略不希望自己的挂单成为当前 best bid，因此不能直接使用原始 `best_bid` 判断，因为原始订单簿中可能已经包含自己的 active/pending 订单。

策略会从 bids 中扣除自己的订单数量后计算竞争对手最高 bid：

```text
competitor_best_bid = 排除自己 active/pending 订单数量后的最高 bid
non_best_cap        = competitor_best_bid - tick
```

`non_best_cap` 是为了“不成为 best_bid”时允许挂的最高价格。

#### target_price 与 best_bid - tick 的冲突处理

当 `target_price` 和 `competitor_best_bid - tick` 冲突时，策略按下面规则选择 `desired_price`：

| 条件 | 动作 |
|---|---|
| `non_best_cap >= target_price` | 按理想价挂单：`desired_price = target_price` |
| `min_reward_price <= non_best_cap < target_price` | 为了避开 best_bid，降到 `desired_price = non_best_cap` |
| `non_best_cap < min_reward_price` | 价格已经低于奖励有效区间：撤掉 active，等待盘口恢复 |
| 没有竞争对手 bid | 不主动挂单；如已有 active，则撤单等待 |

也就是说，策略优先跟随 `mid - offset / 2`，但不会为了跟随 mid 而顶成 best bid；如果避开 best bid 会导致订单掉出奖励有效区间，就取消当前订单并等待。

#### 下单、替换和等待状态机

每个 token 的状态包含：

- `active_order`：当前已挂出的本地订单。
- `pending_replacement`：等待旧单取消后补上的新订单。
- `cancel_requested`：是否已经请求取消 active。
- `last_mid` / `last_best_bid` / `last_best_ask`：最近一次行情快照。
- `halted`：成交后终止策略的保护状态。

每次行情更新后的决策：

| 当前状态 | 决策 |
|---|---|
| 没有 active，也没有 pending，且存在合法 `desired_price` | 直接下新买单 |
| 有 active，且 `active.price != desired_price` | 创建 pending replacement，并请求取消 active |
| 有 active，且 `active.price == desired_price` | 保持不变 |
| 已有 pending replacement | 等待旧 active 的取消确认，不重复发新单 |
| 无法得到合法 `desired_price`，但有 active | 发送 cancel-only，撤掉 active 后等待 |
| 无法得到合法 `desired_price`，且没有 active | 继续等待 |

replacement 流程是异步的：策略先发出 `LiquidityRewardStageReplacement`，订单执行模块请求取消旧 active；收到旧单 `canceled` 状态后，策略才会把 pending replacement 下出去。这样可以避免同一个 token 上同时暴露两个替换订单。

#### 成交终止保护

如果 liquidity_reward 的任一订单发生成交，部分成交也算，策略会进入终止保护：

1. 标记相关状态为 `halted`。
2. 不再根据行情继续补单或 replacement。
3. 对仍存在的 active/pending 订单发起取消。
4. 已部分成交的订单也会尝试取消剩余数量。

该逻辑用于避免成交后继续做市导致仓位继续扩大。

#### 关键订单日志 reason

订单日志中的 `reason` 可用于判断策略为什么行动：

| reason | 含义 |
|---|---|
| `no_order` | 当前没有订单，按最新 desired price 新挂单 |
| `target_price_changed` | active 价格与最新 desired price 不一致，触发替换 |
| `unchanged` | active 已在当前 desired price，保持不动 |
| `pending_replacement` | 已经有待补单，等待旧单取消确认 |
| `outside_reward_zone_wait` | 避开 best_bid 后会掉出奖励区间，撤单或等待 |
| `no_competitor_bid_wait` | 没有可参考的竞争对手 bid，撤单或等待 |
| `invalid_target_price` | 目标价格非法，撤单或等待 |

---

## 数据库表结构

项目使用两个 SQLite 库，避免大体量行情/raw 数据和真实订单恢复状态共用同一个 WAL：

- 订单库：`[app] sqlite_path`，默认 `orders.db`。
- 行情/模拟库：`[app] market_sqlite_path`；为空时默认派生为订单库同目录下的 `market.db`。

### 订单库表

| 表名 | 写入时机 | 说明 |
|---|---|---|
| `orders` | 每次下单/状态变更 | 核心订单表；`status` 值：`pending` / `open` / `filled` / `canceled` / `rejected` |
| `order_events` | 订单生命周期事件 | 订单流水，`payload_json` 存储原始事件 JSON；订单 WS 的 `ws_update` 也在这里，用于计算成交增量 |
| `strategy_state_mid_requote` | 做市策略状态变更 | 做市策略共享状态快照，用于崩溃恢复 |
| `strategy_state_mid_requote_side` | 做市策略状态变更 | 做市策略按 token/side 保存 active、pending、cancel_requested 等状态，PK 为 `(token, side)` |

### 行情/模拟库表

| 表名 | 写入时机 | 说明 |
|---|---|---|
| `liquidity_reward_scores` | `reward_estimator_enabled=true` | 做市奖励得分记录；`my_orders` 为评估时刻内存中关联挂单数量（本地估算值，非 API 数据）；`competitors_qmin=0` 通常表示无竞争者订单满足 `reward_max_spread_cents` 门槛 |
| `market_ticks` | `tick_store_enabled=true` | best bid/ask 变化时的 tick 记录；price/size 精度为 1/10000 的整数 |
| `book_snapshots` | `raw_store_enabled=true` | 全量订单簿快照；`bids`/`asks` 为 BLOB，格式：每档 6 字节 = `price(u16 LE)` + `size(u32 LE)` |
| `trade_events` | `raw_store_enabled=true` | `last_trade_price` 成交事件；字段：`token`、`market`、`price`、`side`、`size`、`fee_rate`、`ts_ms` |

旧版本已经写在订单库里的行情表不会自动迁移或删除；新版本只保证新增行情/模拟/奖励估算数据写入行情库。

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
