# poly-executor

Polymarket 预测市场自动化交易执行器，支持**配对套利**和**流动性奖励做市**两种策略，通过 WebSocket 实时订阅行情，并将真实订单状态与行情/模拟分析数据分库存储到 SQLite。

---

## 目录

- [功能概览](#功能概览)
- [当前整体情况](#当前整体情况)
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

## 当前整体情况

当前工程以 Polymarket 自动化交易为主，主流程由 `main.rs` 串起：加载配置，初始化订单库和行情库，启动恢复流程，创建策略，连接公开行情 WebSocket、私有订单 WebSocket、持仓刷新、订单执行和可选的奖励市场池任务。

### 运行链路

```text
公开行情 WS -> market.rs 本地订单簿 -> dispatcher.rs -> 策略
策略 OrderSignal -> order.rs -> Polymarket CLOB / 模拟成交 -> orders.db
私有订单 WS -> order_ws.rs -> orders.db / 策略成交事件 / 钉钉通知
启动恢复 -> recovery.rs -> orders.db + 远端 open/trades -> OrderCorrelationMap + 策略状态
奖励市场加载/监听 -> reward_market_cache.rs / reward_market_pool_monitor.rs -> market.db
```

### 当前策略

- `PairArbitrage`：读取 `assets.csv`，发现 `1 - (ask0 + ask1) > min_diff` 后发出配对套利信号。
- `LiquidityReward`：读取 CSV 或 DB selected pool，按 token pair 做流动性奖励挂单。策略只主动挂买单；任一侧成交后会 halt 整对 token，撤掉未成交买单，并尝试用 FAK 卖单 unwind 已成交仓位。

### LiquidityReward 当前状态机重点

- 每个 token 正常只允许一张 active 买单。
- 换价采用两阶段：先写 `pending_replacement` 并请求撤 active，收到旧单 `canceled` 后才 promote 新单。
- `token1/token2` 必须属于同一市场 pair；配错会导致 halt、unwind 和单边状态串到错误市场。
- 成交由私有订单 WS 或下单即成交状态驱动；如果远端订单没有本地 `orders` 记录和 correlation，系统无法自动发钉钉、无法触发 halt/unwind。
- 删除本地 `orders.db` 不会取消远端 open orders；远端孤儿单仍可能占用余额并继续成交。
- 遇到 `not enough balance / allowance` 后，策略进入账户级余额冷却，暂停新买单和 pending promote；撤单和 unwind 不受冷却影响。

### 当前流动性奖励市场来源

`[liquidity_reward] source` 控制策略市场来源：

- `csv`：默认值，从 `liquidity_reward.csv` 读取固定市场。
- `db_pool`：从行情库 `reward_market_pool_state` 中读取 `liquidity_reward_selected=1`、仍在池内、且当前 `pool_version` 未被 halt 的市场。

即使策略来源仍是 `csv`，当 `liquidity_reward.enabled=true`、`monitor_enabled=true` 且非全局模拟模式时，奖励市场 loader/monitor 仍会启动并维护 `market.db` 中的奖励市场池；它不一定会被策略采用，除非 `source="db_pool"`。

### 奖励市场池当前规则

奖励市场池每天按 UTC 日期构建一次，成功后等待下一个 UTC 零点；失败会保留旧数据并按监控间隔重试。当前默认筛选规则：

1. 每日奖励 `market_daily_reward > 50`。
2. 距离市场结束时间大于 48 小时。
3. 按 `market_competitiveness` 排序，去掉前 20% 和后 20%。
4. 从剩余市场中选择可配置数量 `pool_market_count`：头部一半和尾部一半写入 `liquidity_reward_selected=1`。
5. pool monitor 订阅 selected/active 市场 token；只检查 `token1`，若 spread 大于 `0.1` 或 best bid 不在 `[0.1, 0.9]`，则把市场标记踢出池，通知策略 halt/cancel/unwind，并发送钉钉剔除通知。
6. UTC 重建池子时，旧 selected 市场如果不在新池子中，会通知策略停止该 token pair；新 selected 市场不会热加入，需等进程下次从 DB pool 构建策略。
7. loader 启动时会读取 DB 中最新 `build_date_utc/pool_version`；如果当天 UTC 池子已构建，则不会因为进程重启立即重刷。

### 数据库边界

- `orders.db` 是订单恢复真相来源，保存本地订单、远端订单 ID、订单事件、策略 active/pending 状态。
- `market.db` 是行情、奖励估算和奖励市场池来源，保存 tick/raw book、Q-score 估算和 pool 状态。
- 启动恢复只会基于本地非终结订单去远端 reconcile；它不会在本地 DB 为空时反向导入所有远端 open orders。
- 真实模式排障时必须同时确认：运行进程、实际配置路径、实际 `sqlite_path`、远端 open orders、本地订单库和订单日志。

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
| `file` | String | `"liquidity_reward.csv"` | CSV 来源下的做市规则文件路径 |
| `source` | String | `"csv"` | 做市市场来源：`csv` 从文件读取；`db_pool` 从 `market.db.reward_market_pool_state` 的 selected 市场读取 |
| `pool_market_count` | usize | `6` | 每日奖励市场池中标记给 liquidity reward 策略使用的市场数量 |
| `monitor_enabled` | bool | `false` | 是否启动奖励监控、奖励市场 loader 和 pool monitor（非模拟模式下生效） |
| `reward_estimator_enabled` | bool | `true` | 是否启用本地 Q-score 奖励估算；开启后写入行情库 `liquidity_reward_scores` 表 |
| `simulation` | bool | `false` | 做市策略内部模拟开关，独立于全局 `[simulation]`；模拟模式下奖励得分写入时会打 `simulation=1` 标记 |
| `balance_cooldown_secs` | u64 | `60` | 余额或 allowance 不足时暂停新买单和 pending promote 的秒数 |

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

### `[notification.dingtalk]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 是否启用钉钉通知 |
| `webhook` | String | `""` | 钉钉机器人完整 webhook；真实值建议放到 `config.local.toml` |
| `secret` | String | `""` | 钉钉机器人加签 secret；为空时不加签 |
| `timeout_secs` | u64 | `5` | 钉钉 HTTP 请求超时秒数 |
| `queue_size` | usize | `1024` | 通知队列长度；队列满时丢弃通知，不阻塞订单 WebSocket |

当前通知触发条件：真实订单 WebSocket 检测到 liquidity_reward 订单成交增量，部分成交也会通知。通知发送失败只写日志，不影响策略 halt、撤单或订单状态更新。

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
| 4 | `reward_max_spread_cents` | f64 | 否 | 奖励有效的最大价差（**单位：cents**，如 `4` 表示 4 cents，即 offset = 0.04）。挂单价格和奖励区间均由此字段计算，建议填整数如 `3` 或 `4`。 |
| 5 | `reward_min_size` | f64 | 否 | 奖励有效的最小单笔挂单金额（USDC） |
| 6 | `reward_daily_pool` | f64 | 否 | 该市场每日奖励总池（USDC），用于估算预期日收益份额 |
| 7 | `fixed_price` | bool | 否 | 报价模式开关：`true` = FixedPrice 模式；空或 `false` = CompetitorBased 模式（默认）。详见下方说明。 |

示例：

```csv
token1,token2,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool,fixed_price
84133519426074676...,53265937461843025...,us_iran,5,4,100,50,        <- CompetitorBased（默认）
12345678901234567...,98765432109876543...,quiet_market,,4,50,30,true  <- FixedPrice
```

---

### LiquidityReward 报价逻辑

策略对 `liquidity_reward.csv` 中配置的每个 token 独立维护一个买单状态。填写 `token1` 和 `token2` 时，分别在 YES token 和 NO token 上挂买单，而不是在同一 token 上挂买卖双边。

#### 实时价格输入

每次收到 market WebSocket 的 `book` 或 `price_change` 后，`market.rs` 维护本地完整订单簿，并将最新 top-of-book 和全量 bids 传给策略：

```text
best_bid = 当前订单簿最高 bid
best_ask = 当前订单簿最低 ask
mid      = (best_bid + best_ask) / 2
```

#### 目标价与奖励有效区间

两种模式共用以下价格计算，基于 `reward_max_spread_cents`（单位 cents，除以 100 得到小数）：

```text
spread           = reward_max_spread_cents / 100
target_price     = snap_to_tick(mid - spread / 2, tick, 向下取整)
min_reward_price = mid - spread
```

- `target_price`：理想挂单价，位于奖励区间内靠近 mid 的一侧。
- `min_reward_price`：奖励有效的最低买单价，低于此值通常不计入 Q-score。

---

#### 模式一：CompetitorBased（默认，`fixed_price` 为空或 `false`）

**适合场景**：交投活跃、对手盘深度好的市场。策略跟随对手盘，不暴露为 best bid，尽量降低被成交的风险。

策略会从 bids 中扣除自己的 active/pending 订单后，计算竞争对手最高 bid：

```text
competitor_best_bid = 排除自己挂单后的最高 bid
non_best_cap        = competitor_best_bid - tick
```

最终 `desired_price` 选取规则：

| 条件 | desired_price | 含义 |
|---|---|---|
| `non_best_cap >= target_price` | `target_price` | 在理想位挂单 |
| `min_reward_price <= non_best_cap < target_price` | `non_best_cap` | 退让至竞争者下方，仍在奖励区间内 |
| `non_best_cap < min_reward_price` | — | 奖励区间外，撤单等待盘口恢复 |
| 无竞争对手 bid | — | 不挂单，如有 active 则撤单等待 |

---

#### 模式二：FixedPrice（`fixed_price = true`）

**适合场景**：交投清淡、几乎无成交的市场。策略按 clean mid 计算 `target_price` 后直接挂单，最大化奖励时间占比。

```text
current_external_bid = 当前 token 排除自己 active/pending 后的最高 bid
paired_external_bid  = 配对 token 排除自己 active/pending 后的最高 bid
current_external_ask = 1 - paired_external_bid
clean_mid            = (current_external_bid + current_external_ask) / 2
desired_price        = target_price（始终）
```

- paired YES/NO 规则使用 `clean_mid` 计算 `target_price`，避免把自己在配对 token 上的买单当成当前 token 的外部 ask。
- mid 每次变化重新计算 `target_price`；若与 active 不同则换单，相同则保持不动。
- 不使用 CompetitorBased 的 `non_best_cap` 退让逻辑；但仍会排除自己的 active/pending 订单来获得外部盘口。
- 如果无法得到 clean mid，则不新挂单；已有 active 时会先撤掉，等待外部盘口恢复。
- 策略有意接受偶发成交的风险，成交后由 halt + 市价平仓逻辑兜底。

---

#### 下单、替换和等待状态机（两种模式共用）

每个 token 的状态：

- `active_order`：当前已挂出的本地订单。
- `pending_replacement`：等待旧单取消后补上的新订单。
- `cancel_requested`：是否已发出取消请求，防重复。
- `last_mid` / `last_best_bid` / `last_best_ask`：最近一次行情快照。
- `halted`：成交后终止策略的保护标志。

每次行情更新后的决策：

| 当前状态 | 决策 |
|---|---|
| 无 active，无 pending，存在合法 `desired_price` | 直接下新买单 |
| 有 active，且 `active.price != desired_price` | 创建 pending replacement，请求取消 active |
| 有 active，且 `active.price == desired_price` | 保持不变 |
| 已有 pending replacement | 等待旧 active 取消确认，不重复发单 |
| 无法得到合法 `desired_price`，有 active | 发送 cancel-only，撤掉后等待 |
| 无法得到合法 `desired_price`，无 active | 继续等待 |

replacement 流程是异步的：策略先发出 `LiquidityRewardStageReplacement`，订单执行模块取消旧 active；收到 `canceled` 后，策略才将 pending replacement 正式下出，避免同一 token 同时暴露两笔替换订单。

---

#### 成交终止保护（两种模式共用）

任一订单发生成交（部分成交也算），策略立即进入终止保护：

1. 取消 token 对（YES + NO）上所有 active/pending 订单。
2. 将两个 token 状态标记为 `halted`，不再补单。
3. 以 `last_best_bid` 为参考价，立即发出市价卖单，平掉成交的持仓（`delta_size`）。

市价卖单价格来自成交时刻的 `last_best_bid`（snap 到 tick）；在流动性薄的市场，实际成交价可能低于参考价，产生一定价差损失，属于已知风险。

真实订单 WebSocket 检测到成交增量时，若启用了 `[notification.dingtalk]`，会异步发送钉钉通知，通知失败不影响 halt 和撤单流程。

当策略来源为 `db_pool` 时，halt 会持久化到当前池子版本：`reward_market_pool_state.liquidity_reward_halted=1`，并记录 `liquidity_reward_halted_pool_version`、`liquidity_reward_halted_at_ms` 和 `liquidity_reward_halt_reason`。进程重启后，同一个 `pool_version` 下已 halted 的市场不会再次进入策略；UTC 重新构建池子生成新 `pool_version` 后，旧 halt 不再阻止该市场重新参与策略。

奖励池 monitor 踢出市场、以及 UTC 重建时旧 selected 市场消失，也会走同一套 halt/cancel/unwind 保护；其中 monitor 踢出还会发送钉钉剔除通知。

---

#### 关键订单日志 reason

| reason | 触发模式 | 含义 |
|---|---|---|
| `no_order` | 两种 | 无订单，按 desired_price 新挂单 |
| `target_price_changed` | CompetitorBased | active 价格与 desired_price 不一致，触发替换 |
| `price_drifted` | FixedPrice | mid 偏移导致 target_price 变化，触发替换 |
| `unchanged` | 两种 | active 已在 desired_price，保持不动 |
| `pending_replacement` | 两种 | 已有待补单，等待旧单取消确认 |
| `outside_reward_zone_wait` | CompetitorBased | 退让后掉出奖励区间，撤单或等待 |
| `no_competitor_bid` | CompetitorBased | 无可参考的竞争对手 bid，撤单或等待 |
| `no_external_fixed_mid` | FixedPrice | 无法从当前 token 外部 bid 和配对 token 外部 bid 计算 clean mid，撤单或等待 |
| `invalid_target_price` | 两种 | 目标价格非法（≤ 0），撤单或等待 |

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
| `reward_market_pool_state` | `monitor_enabled=true` 且非模拟模式 | 每日奖励市场池状态；包含 token1/token2、competitiveness、奖励参数、是否仍在池、踢出原因、是否被选入 liquidity reward 策略，以及当前池子版本内是否已被 halt |

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
| `monitor.rs` | 实时估算做市奖励得分；监控 liquidity reward 相关订单和行情 |
| `reward_market_cache.rs` | 拉取 Polymarket 当前奖励市场和市场详情，按规则构建每日奖励市场池并写入行情库 |
| `reward_market_pool_monitor.rs` | 动态订阅奖励市场池行情，按 token1 spread 和 bid 区间把不合格市场踢出池 |
| `recovery.rs` | 启动时从 SQLite 恢复订单关联关系和做市挂单状态，并和远端订单/成交保守对账 |
| `storage.rs` | SQLite 封装；订单库与行情库分离，使用 WAL + NORMAL sync 模式 |
| `proxy_ws.rs` | SOCKS5 / HTTP 代理 WebSocket 连接层 |
| `polymarket_rewards.rs` | Polymarket 官方 Q-score 做市奖励积分计算算法 |
