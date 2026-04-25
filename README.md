# poly-executor

Polymarket 自动化执行器，用 Rust/Tokio 实现行情订阅、策略分发、订单执行、仓位同步、SQLite 持久化和奖励监控。

当前项目包含两个策略：

- `pair_arbitrage`：根据 `assets.csv` 中的 token 配对，监控 `1 - (ask0 + ask1)` 的套利空间。
- `mid_requote`：根据 `mid.csv` 中的 token 配置，在盘口中间价附近维护双边库存报价。

## 架构概览

```text
Polymarket WS
    │
    ▼
market
  - 维护本地 orderbook
  - 提取 best bid / best ask
  - 按 token/topic 发送 MarketEvent
    │
    ▼
dispatcher
  - 每个策略独立 mpsc 队列
  - 慢策略不会阻塞其他策略消费
    │
    ├── pair_arbitrage
    │     └── 发现套利机会后发送 OrderSignal
    │
    └── mid_requote
          ├── 根据行情维护 buy/sell lane
          ├── 根据仓位同步卖侧库存
          └── 发送下单、替换、撤单信号

OrderSignal
    │
    ▼
order
  - simulation 模式：本地模拟成交
  - real 模式：调用 Polymarket CLOB 下单/撤单
  - 写入 orders / order_events
    │
    ├── order_ws：真实订单状态回报
    └── positions：真实或模拟仓位同步
```

## 核心模块

| 模块 | 作用 |
| --- | --- |
| `src/main.rs` | 启动入口，加载配置、初始化日志/SQLite、组装策略和任务。 |
| `src/config.rs` | 读取 `config.toml` 和可选的 `config.local.toml`。 |
| `src/market.rs` | 订阅 Polymarket 行情，维护本地 orderbook，输出标准化 `CleanOrderbook`。 |
| `src/dispatcher.rs` | 将行情、仓位、订单状态事件分发到各策略队列。 |
| `src/strategy.rs` | 策略公共类型、事件、订单信号和注册信息。 |
| `src/strategies/pair_arbitrage.rs` | pair arbitrage 策略。 |
| `src/strategies/mid_requote.rs` | mid requote 双边库存策略。 |
| `src/order.rs` | 执行策略下单信号，支持模拟和真实下单。 |
| `src/order_ws.rs` | 监听真实订单 websocket，更新订单状态并触发策略事件。 |
| `src/positions.rs` | 同步真实仓位或维护模拟仓位。 |
| `src/recovery.rs` | 启动时从 SQLite 和交易所恢复订单关联与策略状态。 |
| `src/storage.rs` | SQLite schema 和订单/事件/策略状态持久化。 |
| `src/monitor.rs` | 用户奖励监控和本地流动性奖励估算。 |
| `src/polymarket_rewards.rs` | 流动性奖励估算公式。 |

## 配置文件

### `config.toml`

```toml
[proxy]
url = ""

[auth]
api_key     = ""
api_secret  = ""
passphrase  = ""
private_key = ""
funder      = ""

[order]
enabled   = false
size_usdc = 10.0

[simulation]
enabled = true

[mid_requote]
enabled = true
file = "mid.csv"
monitor_enabled = true

[app]
log_file = "alerts.log"
order_log_file = "orders.log"
assets_file = "assets.csv"
min_diff = 0.01
max_spread = 0.05
min_price = 0.02
max_price = 0.98
default_threads = 3
monitor_interval_secs = 30
sqlite_path = "orders.db"

[topic_threads]
# topic = connection_count
```

配置加载顺序：

1. `config.toml`
2. `config.local.toml`

`config.local.toml` 会覆盖同名配置，适合存放私钥、API key、本地路径等敏感或环境相关配置。

### 重要开关

| 配置 | 说明 |
| --- | --- |
| `order.enabled` | 是否允许真实订单执行。为 `false` 时不会向交易所下单。 |
| `simulation.enabled` | 是否启用本地模拟成交/仓位。为 `true` 时不走真实 order websocket 和真实 positions。 |
| `mid_requote.enabled` | 是否启用 `mid_requote` 策略。 |
| `mid_requote.monitor_enabled` | 是否启用当前用户奖励监控。只在非 simulation 模式下启动。 |
| `app.sqlite_path` | SQLite 订单数据库路径。 |

## CSV 配置

### `assets.csv`

用于 `pair_arbitrage` 策略。

```csv
token0,token1,topic
<token0>,<token1>,fifa
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `token0` | 第一个 outcome token。 |
| `token1` | 第二个 outcome token。 |
| `topic` | 主题名，用于订阅分组和策略路由。 |

### `mid.csv`

用于 `mid_requote` 策略。

```csv
token,offset,mid_change_threshold,target_order_size,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool
<token>,0.001,0.002,100,mid,,4,100,11
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `token` | 要维护报价的 Polymarket token id。 |
| `offset` | 报价偏移。买价为 `best_bid - offset`，卖价为 `best_ask + offset`。 |
| `mid_change_threshold` | 中间价变化阈值。超过阈值才触发对应 side 的换价。 |
| `target_order_size` | 目标库存/买侧目标。买侧目标数量为 `target_order_size - 当前库存`。 |
| `topic` | 主题名，默认 `mid`。 |
| `reward_min_orders` | 奖励配置字段，目前用于配置透传，本地估算中未强制过滤。 |
| `reward_max_spread_cents` | 奖励最大价差，单位为 cents，用于本地奖励估算。 |
| `reward_min_size` | 奖励最小 size 配置，目前用于配置透传，本地估算中未强制过滤。 |
| `reward_daily_pool` | 日奖励池，用于估算当前报价占比对应的每日奖励。 |

## `mid_requote` 策略逻辑

`mid_requote` 是双边库存策略，每个 token 独立维护 buy lane 和 sell lane。

### 定价

```text
mid = (best_bid + best_ask) / 2
buy_price = best_bid - offset
sell_price = best_ask + offset
```

策略只使用收到的新行情快照报价；启动恢复时不会复用 SQLite 中旧的 `last_best_bid / last_best_ask` 直接下单。

### 买侧

```text
buy_size = target_order_size - current_position_size
```

- 如果 `buy_size > 0`，买侧维持一笔 buy 报价。
- 如果 mid 相对买侧上次报价 mid 的变化超过 `mid_change_threshold`，则撤旧买单并挂新买单。
- 如果库存达到或超过目标，买侧撤单。

### 卖侧

```text
sell_size = current_position_size
```

- 只有当前库存大于 0 时才挂卖单。
- 卖单数量等于当前库存。
- 如果库存变化，卖侧会按最新库存调整 size。
- 如果库存归零，卖侧撤单。
- 如果 mid 相对卖侧上次报价 mid 的变化超过 `mid_change_threshold`，则撤旧卖单并挂新卖单。

注意：如果卖单挂在 `best_ask + offset` 后自己成为新的 best ask，网页上看到它位于 best ask 是正常现象。

## `pair_arbitrage` 策略逻辑

`pair_arbitrage` 从 `assets.csv` 加载 token 配对，并对每个 pair 计算：

```text
gap = 1 - (ask0 + ask1)
```

当满足以下条件时输出套利信号：

- `gap > app.min_diff`
- 两边盘口价差不超过 `app.max_spread`
- bid/ask 位于 `[app.min_price, app.max_price]`

当前套利信号会进入统一订单链路。

## 订单、仓位与恢复

### SQLite 表

程序启动时会初始化 SQLite，主要表包括：

| 表 | 说明 |
| --- | --- |
| `orders` | 本地订单主表，记录 local/remote order id、策略、token、side、price、size、status。 |
| `order_events` | 订单生命周期事件，便于回溯下单、撤单、websocket 更新等。 |
| `strategy_state_mid_requote` | `mid_requote` token 级共享状态，如 last mid、last bid/ask、last position。 |
| `strategy_state_mid_requote_side` | `mid_requote` buy/sell side 状态，如 active order、pending replacement、cancel_requested。 |

### 启动恢复

启动时 `RecoveryCoordinator` 会：

1. 从 SQLite 加载本地 active orders 和策略状态。
2. 非 simulation 模式下拉取交易所 open orders。
3. 对本地订单和远端订单做对账。
4. 重建 local/remote order id 到本地订单元数据的关联。
5. 恢复 `mid_requote` 的 buy/sell lane 状态。

恢复后，策略会等待新的行情快照再报价，避免用历史盘口直接下单。

## 奖励监控

项目有两类奖励相关日志：

1. 当前用户奖励监控：调用 Polymarket API 查询当前用户奖励信息。
2. 本地流动性奖励估算：根据当前 orderbook、自己的报价和 `mid.csv` 中的奖励配置估算奖励份额。

本地估算核心公式位于 `src/polymarket_rewards.rs`，会计算：

```text
score = ((max_spread - spread) / max_spread)^2
qmin  = two-sided 或 single-sided penalty 后的有效流动性
share = my_qmin / (my_qmin + competitors_qmin)
estimated_reward = share * reward_daily_pool
```

## 运行

### 编译

```bash
cargo build
```

### 模拟模式运行

适合本地验证策略闭环，不会真实下单。

```toml
[simulation]
enabled = true

[order]
enabled = false
```

运行：

```bash
cargo run
```

### 实盘模式运行

实盘前请确认：

```toml
[simulation]
enabled = false

[order]
enabled = true
```

并在 `config.local.toml` 中配置：

```toml
[auth]
api_key     = "..."
api_secret  = "..."
passphrase  = "..."
private_key = "0x..."
funder      = "0x..."
```

然后运行：

```bash
cargo run --release
```

## 日志

默认日志文件：

| 文件 | 说明 |
| --- | --- |
| `alerts.log` | 策略告警日志。 |
| `orders.log` | 订单、行情诊断、奖励监控等订单相关日志。 |

日志路径可通过 `config.toml` 的 `app.log_file` 和 `app.order_log_file` 配置。

## 开发注意事项

- 配置文件中的私钥/API key 不要提交到 Git。
- `orders.db`、WAL 文件和临时备份文件不应提交。
- 实盘前先使用 simulation 模式验证配置和策略行为。
- `mid_requote` 的 `target_order_size` 是目标库存逻辑，不是每次固定买入数量。
- 当前本地奖励估算基于内存订单关联和本地 orderbook，适合辅助观察，不应当作为最终结算依据。
