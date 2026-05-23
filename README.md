# poly-executor

Polymarket 自动化交易执行器。当前项目以 **公开行情订阅、策略计算、订单网关、仓位管理、全局风控** 为核心链路，支持配对套利告警、做市策略模拟/发单、订单恢复、仓位估值、单日亏损停止和行情录制。

---

## 目录

- [当前整体情况](#当前整体情况)
- [运行链路](#运行链路)
- [策略](#策略)
- [订单网关](#订单网关)
- [仓位与估值](#仓位与估值)
- [风控](#风控)
- [行情订阅与存储](#行情订阅与存储)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [CSV 文件格式](#csv-文件格式)
- [数据库与持久化](#数据库与持久化)
- [工具命令](#工具命令)
- [模块说明](#模块说明)
- [已知限制](#已知限制)

---

## 当前整体情况

项目当前围绕 Polymarket 的 YES/NO token 运行：

- 行情层按 **token = topic** 分发，策略只订阅自己关心的 token。
- 策略层当前包含 `pair_arbitrage` 和 `market_maker`。
- 订单统一通过 `OrderGateway`，策略不直接访问交易所。
- 仓位由订单事件驱动，维护全局和策略级 token 仓位。
- 风控在 gateway 层统一拦截新下单，并允许撤单继续执行。
- 模拟模式由 `[simulation].enabled` 控制；开启后不启动真实私有订单 WS 和 settlement poller。
- 配置文件从 `config.toml` 读取，再由 `config.local.toml` 覆盖，敏感配置应放在本地覆盖文件。

---

## 运行链路

```text
config.toml + config.local.toml
        |
        v
main.rs 初始化日志、SQLite、账户监控、策略、风控、OrderGateway
        |
        +--> market.rs 公开行情 WS
        |        |
        |        +--> 本地订单簿 / MarketBookReadHandle
        |        +--> token topic broadcast -> 策略
        |        +--> tick/raw recorder -> market.db
        |
        +--> strategy::{pair_arbitrage, market_maker}
        |        |
        |        +--> OrderRequest::{Place, Cancel, Query}
        |
        +--> order::order_gateway
        |        |
        |        +--> GatewayRiskEngine
        |        +--> simulation 或真实 CLOB 下单
        |        +--> OrderEventRing
        |        +--> orders.db gateway 快照和事件
        |
        +--> order::order_ws / settlement activity poller（非 simulation）
        |        |
        |        +--> GatewayObservation -> OrderGateway
        |
        +--> position::position_engine
                 |
                 +--> position journal/snapshot
                 +--> position read handle -> 策略和风控
```

---

## 策略

### PairArbitrage

`PairArbitrageStrategy` 从 `assets.csv` 读取 token 对，维护每个 token 最新 best bid/ask：

```text
套利空间 = 1 - (ask_token0 + ask_token1)
```

当套利空间大于 `[app].min_diff`，且价格区间和 bid/ask spread 满足配置过滤条件时，当前只输出套利信号日志，不直接发真实订单。

### MarketMaker

`MarketMakerStrategy` 从 `market_maker.csv` 读取 YES/NO token pair。每个市场会同时观察 YES 和 NO 两边订单簿，并生成买单报价。

当前核心计算：

1. 对每个 token 从订单簿计算 fair midpoint。
2. 用 YES/NO 各自 fair midpoint 对当前仓位估值。
3. 计算库存偏斜：
   - YES 偏多时，让 NO 更容易买到。
   - NO 偏多时，让 YES 更容易买到。
4. 生成多档目标买单。
5. 通过策略内风控检查后，向 `OrderGateway` 投递 `PlaceOrderRequest`。

报价参数来自 `[market_maker]` 配置；其中 CSV 的 `rewards_max_spread` 和 `reward_min_size` 是市场级参数，会优先覆盖全局默认值。

### MarketMaker 策略内风控和冷静期

做市策略下单前会执行内部 quote risk：

| 风控码 | 触发条件 | 冷静期 |
|---|---|---:|
| `abnormal_market_spread` | YES/NO 组合盘口 spread 异常 | 1 分钟 |
| `fair_midpoint_out_of_range` | fair midpoint 超出安全价格区间 | 10 分钟 |
| `price_volatility` | 5 分钟窗口内 fair midpoint 波动超过阈值 | 5 分钟 |

触发任一规则后：

1. 市场进入冷静期。
2. 立即向 `OrderGateway` 投递撤单请求。
3. 冷静期内跳过该市场所有新报价。
4. 冷静期结束后重新评估；如果风险仍存在，会再次进入冷静期并再次撤单。

撤单范围：

- `market_maker.csv` 中有 `condition_id`：按 `CancelScope::Market` 撤掉该市场订单。
- 没有 `condition_id`：退化为按 `token1` 和 `token2` 分别发送 `CancelScope::Token`。

---

## 订单网关

`OrderGateway` 是策略和交易所之间的唯一入口。

策略只投递：

- `OrderRequest::Place`
- `OrderRequest::Cancel`
- `OrderRequest::Query`

gateway 负责：

- 启动恢复本地未终结订单。
- 统一调用 `GatewayRiskEngine`。
- 在 simulation 模式下生成本地模拟订单事件。
- 在真实模式下执行 CLOB 下单。
- 接收私有订单 WS observation。
- 接收 Data API settlement activity confirmation。
- 发布 `OrderEventRing` 事件给仓位引擎。
- 将 gateway 快照、事件和提交记录写入 SQLite。

当前 cancel scope：

| Scope | 说明 |
|---|---|
| `LocalOrderId` | 撤单个本地订单 |
| `Token` | 撤某个 token 下该策略的非终结订单 |
| `Market` | 撤某个 market/condition 下该策略的非终结订单 |
| `AllForStrategy` | 撤该策略全部非终结订单 |

注意：当前 gateway 已有本地取消状态处理；真实 Polymarket REST 撤单接口仍需要继续接入。

---

## 仓位与估值

`position::position_engine` 是单写者仓位引擎，通过 `OrderEventRing` 消费订单事件，维护：

- strategy + token 仓位
- global + token 仓位
- working buy exposure
- filled position
- cost basis
- realized pnl
- degraded 状态

估值位于 `position::valuation`：

```text
market_value   = filled_position * mark_price
unrealized_pnl = market_value - cost_basis
total_pnl      = realized_pnl + unrealized_pnl
```

当前只做 token 级和 portfolio 汇总估值，不做 YES/NO 配对 matched/unmatched 估值。

`PortfolioValuation` 会汇总：

- `market_value`
- `cost_basis`
- `realized_pnl`
- `unrealized_pnl`
- `total_pnl`
- `missing_price_tokens`
- `degraded`

全局风控使用 `MarkPriceKind::BestBid` 作为 mark price，偏保守。

---

## 风控

风控位于 `risk::risk`，由 `GatewayRiskEngine` 在下单前执行。

当前已有规则：

| 规则 | 说明 |
|---|---|
| PositionEngineHealthRule | 仓位引擎未 live 时拒绝新下单，允许撤单 |
| DailyLossLimitRule | 单日亏损达到阈值后拒绝当天所有新下单 |
| BasicOrderSanityRule | 基础订单参数检查 |
| StrategyKindOrderSizeRule | 按策略类型限制单笔订单大小 |
| GlobalTokenExposureRule | 按 token 限制全局风险暴露 |
| StrategyKindTokenExposureRule | 按策略类型限制 token 暴露 |

### 单日亏损停止

默认开启：

```text
loss_limit_ratio = 0.03
```

第一版账户权益定义为账户 collateral balance。当天第一次有可用估值和账户权益时，风控持久化：

```text
day_start_total_pnl
day_start_equity
loss_limit_amount = day_start_equity * 0.03
```

之后每次新下单计算：

```text
daily_pnl = current_total_pnl - day_start_total_pnl
```

如果：

```text
daily_pnl <= -loss_limit_amount
```

则写入 `risk_daily_loss_state.halted = 1`，并拒绝当天所有新下单。

重启保护：

- 当天已经 halted：重启后仍拒绝新下单。
- 估值缺价格或 degraded：保守拒绝新下单。
- 账户权益不可用或非正数：保守拒绝新下单。
- cancel 请求不受 daily loss halt 影响。

---

## 行情订阅与存储

`market.rs` 连接 Polymarket 公开行情 WS，维护每个 token 的本地订单簿。

当前分发模型：

```text
token_id == topic
```

策略注册自己关心的 token，`main.rs` 为每个 token 创建 broadcast channel。公开行情收到某个 asset/token 的更新后：

1. 更新本地 `MarketBookPublisher`。
2. 发送 firehose 事件。
3. 发送该 token topic 的策略事件。

行情内部仍使用固定精度整数：

| 字段 | 类型 | 精度 |
|---|---|---|
| price | `u16` | 除以 `10000` 得到实际价格 |
| size | `u32` | 除以 `10000` 得到实际数量 |

策略计算层当前仍沿用固定精度数据；只有日志、下单、存储、展示等边界会转换成实际精度。

可选 recorder：

- `tick_store_enabled=true`：写入 best bid/ask tick。
- `raw_store_enabled=true`：写入全量订单簿快照和 trade event。

---

## 快速开始

1. 准备配置：

```bash
cp config.toml config.local.toml
```

在 `config.local.toml` 填写真实 `auth`、代理、数据库路径等本地配置。不要提交真实密钥。

2. 准备 CSV：

```text
assets.csv          # pair_arbitrage token 对
market_maker.csv    # market_maker YES/NO token 对
```

3. 编译：

```bash
cargo build --release
```

4. 运行：

```bash
./target/release/poly-executor
```

Windows PowerShell 下通常是：

```powershell
.\target\release\poly-executor.exe
```

---

## 配置说明

### `[chain]`

| 配置项 | 类型 | 默认/示例 | 说明 |
|---|---|---|---|
| `rpc_url` | String | `https://polygon-rpc.com` | Polygon RPC，用于 `merge` 工具等链上操作 |

### `[proxy]`

| 配置项 | 类型 | 说明 |
|---|---|---|
| `url` | String | 代理地址，例如 `socks5://127.0.0.1:7890`；为空时按代码路径使用直连或环境代理能力 |

### `[auth]`

| 配置项 | 类型 | 说明 |
|---|---|---|
| `api_key` | String | Polymarket CLOB API Key |
| `api_secret` | String | Polymarket CLOB API Secret |
| `passphrase` | String | Polymarket CLOB API Passphrase |
| `private_key` | String | EIP-712 签名私钥 |
| `funder` | String | 钱包地址 |

### `[order]`

| 配置项 | 类型 | 说明 |
|---|---|---|
| `size_usdc` | f64 | pair arbitrage 每次套利信号对应的 USDC 尺寸参数 |

### `[simulation]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `enabled` | bool | `false` | 是否启用全局模拟模式。`true` 时不走真实私有订单 WS、settlement poller 和真实下单链路 |

### `[app]`

| 配置项 | 类型 | 说明 |
|---|---|---|
| `log_file` | String | 日志文件路径 |
| `assets_file` | String | `assets.csv` 路径 |
| `sqlite_path` | String | 订单库路径，默认常用 `orders.db` |
| `market_sqlite_path` | String | 行情库路径；为空时派生为订单库同目录下 `market.db` |
| `min_diff` | f64 | pair arbitrage 触发阈值 |
| `max_spread` | f64 | pair arbitrage 单 token 最大 bid/ask spread |
| `min_price` | f64 | pair arbitrage 有效价格下限 |
| `max_price` | f64 | pair arbitrage 有效价格上限 |
| `default_threads` | usize | 历史订阅线程参数；当前 token topic 路由下实际作用有限 |
| `tick_store_enabled` | bool | 是否记录 best tick |
| `raw_store_enabled` | bool | 是否记录 raw book snapshot 和 trade event |

### `[market_maker]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `enabled` | bool | `true` | 是否启动做市策略；`false` 时不会读取 `market_maker.csv` |
| `file` | String | `market_maker.csv` | 做市市场 CSV 文件路径；相对路径按可执行文件目录解析 |
| `max_inventory_usd` | f64 | `100.0` | 单市场库存归一化上限，单位 USD；用于计算 `inventory_ratio` |
| `overweight_ratio` | f64 | `0.7` | 库存比例超过该值认为超重，超重时减少报价档位 |
| `default_max_spread` | f64 | `0.03` | CSV 未配置 `rewards_max_spread` 时使用的默认最大报价偏离 |
| `tick_size` | f64 | `0.01` | 报价价格 tick，目标买价会向下取整到该 tick |
| `min_size` | f64 | `5.0` | CSV 未配置 `reward_min_size` / `rewards_min_size` 时使用的默认最小 token 数量 |
| `max_skew` | f64 | `0.01` | 库存偏斜对报价 `adjusted_mid` 的最大调整幅度 |
| `volatility_window_ms` | u64 | `300000` | fair midpoint 波动率统计窗口，单位毫秒 |
| `volatility_min_samples` | usize | `5` | 波动率检测所需最少样本数 |
| `volatility_threshold` | f64 | `0.02` | 窗口内 fair midpoint 最大值和最小值差超过该阈值时触发冷静期 |
| `spread_cooldown_ms` | u64 | `60000` | 市场盘口 spread 异常后的冷静期，单位毫秒 |
| `volatility_cooldown_ms` | u64 | `300000` | fair midpoint 波动率过高后的冷静期，单位毫秒 |
| `fair_midpoint_cooldown_ms` | u64 | `600000` | fair midpoint 超出安全价格区间后的冷静期，单位毫秒 |
| `fair_midpoint_min` | f64 | `0.15` | fair midpoint 安全区间下限 |
| `fair_midpoint_max` | f64 | `0.85` | fair midpoint 安全区间上限 |
| `abnormal_market_spread_multiplier` | f64 | `2.0` | 市场盘口 spread 大于 `max_spread * multiplier` 时认为异常 |
| `normal_quote_levels` | usize | `3` | 正常库存状态下最多报价档位数 |
| `overweight_quote_levels` | usize | `2` | 库存超重状态下最多报价档位数 |
| `level_ratios` | Vec<f64> | `[0.4, 0.55, 0.7]` | 每档距离 fair midpoint 的比例：`distance = max_spread * level_ratio` |
| `level_sizes_usd` | Vec<f64> | `[50.0, 75.0, 100.0]` | 每档目标美元金额：`size = max(level_size_usd / price, min_size)` |
| `reconcile_size_tolerance` | f64 | `0.2` | reconcile 判断当前挂单 size 接近目标 size 的容忍比例 |

优先级：`market_maker.csv` 的 `rewards_max_spread` 覆盖 `default_max_spread`，`reward_min_size` / `rewards_min_size` 覆盖 `min_size`。`PRICE_SCALE`、`MARKET_MAKER_NAME`、风控 code 和 `local_order_id` 格式是内部协议，不是配置项。

### `[notification.dingtalk]`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `enabled` | bool | `false` | 是否启用钉钉通知 |
| `webhook` | String | `""` | 钉钉机器人 webhook |
| `secret` | String | `""` | 钉钉加签 secret；为空时不加签 |
| `timeout_secs` | u64 | `5` | HTTP 请求超时 |
| `queue_size` | usize | `1024` | 通知队列长度 |

### `[liquidity_reward]`

配置结构仍存在，部分代码也仍保留奖励市场池 loader/monitor 和历史状态表；当前主流程的做市策略加载入口是 `market_maker.csv`。

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `enabled` | bool | `false` | 历史 liquidity reward 开关 |
| `file` | String | `""` | 历史 CSV 文件路径 |
| `source` | String | `csv` | 历史市场来源 |
| `pool_market_count` | usize | `6` | 奖励市场池选入数量 |
| `pool_max_rewards_min_size` | Option<f64> | `None` | 奖励池筛选参数 |
| `monitor_enabled` | bool | `false` | 是否启动奖励市场池监控 |
| `simulation` | bool | `false` | 历史策略内部模拟开关 |
| `balance_cooldown_secs` | u64 | `60` | 历史余额冷却参数 |

---

## CSV 文件格式

### `assets.csv`

用于 `pair_arbitrage`，要求有表头。

| 列 | 字段 | 必填 | 说明 |
|---|---|---|---|
| 0 | `token0` / 任意表头 | 是 | 第一个 token asset ID |
| 1 | `token1` / 任意表头 | 是 | 第二个 token asset ID |
| 2 | `topic` / 任意表头 | 否 | 历史 topic 字段；当前订阅实际按 token 自身注册 |

示例：

```csv
token0,token1,topic
84133519426074676...,53265937461843025...,us_iran
```

### `market_maker.csv`

用于 `MarketMakerStrategy`，要求有表头。

当前解析字段：

| 字段 | 必填 | 说明 |
|---|---|---|
| `condition_id` | 否 | Polymarket 市场 condition ID；存在时用于 market 级撤单和 local order id |
| `token1` | 是 | YES 或第一侧 token asset ID |
| `token2` | 是 | NO 或第二侧 token asset ID |
| `rewards_max_spread` | 否 | 市场级最大报价偏离；有值时覆盖 `[market_maker].default_max_spread` |
| `reward_min_size` / `rewards_min_size` | 否 | 市场级最小 token 数量；有值时覆盖 `[market_maker].min_size` |

兼容说明：

- 如果没有 `condition_id` 列，会按旧格式读取前两列作为 `token1` 和 `token2`。
- 如果有 `condition_id`，冷静期撤单使用 `CancelScope::Market`。
- 如果没有 `condition_id`，冷静期撤单使用两个 token scope。
- 做市行为参数来自 `[market_maker]`；CSV 只保留市场级覆盖项。

示例：

```csv
condition_id,token1,token2
0xabc...,84133519426074676...,53265937461843025...
```

旧格式仍可用：

```csv
token1,token2
84133519426074676...,53265937461843025...
```

---

## 数据库与持久化

项目默认使用两个 SQLite 文件：

| 数据库 | 默认路径 | 说明 |
|---|---|---|
| 订单库 | `orders.db` | 订单、gateway、仓位、全局风控状态 |
| 行情库 | `market.db` | tick、raw book、trade event、奖励市场池 |

### 订单库关键表

| 表 | 说明 |
|---|---|
| `order_gateway_snapshots` | gateway 订单状态快照 |
| `order_gateway_events` | gateway 订单事件日志 |
| `order_gateway_submissions` | 下单提交记录 |
| `position_journal` | 仓位事件流水 |
| `position_snapshots` | 仓位快照 |
| `position_open_orders` | 仓位侧 working exposure 快照 |
| `position_reconciliations` | 仓位对账记录 |
| `risk_daily_loss_state` | 单日亏损风控状态，支持重启后继续 halted |
| `strategy_state_mid_requote*` | 历史 liquidity reward/mid requote 状态表 |

### 行情库关键表

| 表 | 说明 |
|---|---|
| `market_ticks` | best bid/ask tick |
| `book_snapshots` | 全量订单簿快照，BLOB 固定格式 |
| `trade_events` | 公开成交事件 |
| `reward_market_pool_state` | 奖励市场池状态 |

`book_snapshots` BLOB 每档 6 字节：

```text
price: u16 little-endian
size : u32 little-endian
```

解析时都需要除以 `10000` 得到实际值。

---

## 工具命令

### `merge`

`src/bin/merge.rs` 用于把同一市场等量 YES + NO 通过 Polygon CTF 合约 merge 回 USDC。

```bash
cargo build --release --bin merge
./target/release/merge <condition_id> <amount_usdc>
```

PowerShell：

```powershell
cargo build --release --bin merge
.\target\release\merge.exe <condition_id> <amount_usdc>
```

参数：

| 参数 | 说明 |
|---|---|
| `condition_id` | Polymarket 市场 condition ID |
| `amount_usdc` | merge 数量，单位 USDC |

---

## 模块说明

| 路径 | 说明 |
|---|---|
| `src/main.rs` | 程序入口，串联配置、日志、SQLite、策略、行情、订单、仓位和风控 |
| `src/config.rs` | 配置结构和加载逻辑 |
| `src/market.rs` | 公开行情 WS、本地订单簿、token topic 分发、行情 recorder |
| `src/order/order_gateway.rs` | 订单网关、风控接入、订单状态机、启动恢复、事件发布和持久化 |
| `src/order/order_ws.rs` | Polymarket 私有订单 WS observation 适配 |
| `src/position/position_engine.rs` | 仓位单写者、读句柄、持久化任务、订单事件桥接 |
| `src/position/valuation.rs` | token 和 portfolio 估值 |
| `src/risk/risk.rs` | gateway 全局/策略类型风控，包括 daily loss |
| `src/strategies/pair_arbitrage.rs` | 配对套利信号策略 |
| `src/strategies/market_maker.rs` | 做市策略、库存偏斜、多档报价、内部风控冷静期 |
| `src/strategies/strategy.rs` | 策略公共类型、订阅注册、market subscription mux |
| `src/storage.rs` | SQLite schema 和读写封装 |
| `src/account.rs` | 账户资金快照监控 |
| `src/clob_client.rs` | 认证 CLOB client 构造 |
| `src/reward_market_cache.rs` | 奖励市场池 loader |
| `src/reward_market_pool_monitor.rs` | 奖励市场池监控和剔除 |
| `src/notification.rs` | 钉钉通知 |
| `src/proxy_ws.rs` | 代理 WebSocket 连接 |
| `src/tick_size.rs` | tick size 查询和价格/数量对齐辅助 |
| `src/bin/merge.rs` | YES/NO merge 工具 |

---

## 已知限制

- `pair_arbitrage` 当前只输出信号，不直接交易。
- `risk::RiskConfig` 当前使用代码默认值，尚未从 `config.toml` 加载。
- `MarketRiskReader` 当前主流程使用 `NoopMarketRiskReader`。
- gateway 当前已有本地取消处理，但真实 Polymarket REST 撤单接口还需要继续接入。
- 单日亏损风控第一版账户权益只使用账户 collateral balance，没有把持仓市值并入 equity。
- portfolio 估值当前按 token 汇总，不做 YES/NO pair matched 估值。
- `topic_threads` 是历史配置；当前策略订阅已简化为 token topic。
- `liquidity_reward` 相关配置和奖励市场池代码仍保留，但当前主做市入口是 `market_maker.csv`。
