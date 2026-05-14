# Market Maker Strategy Skeleton Design

## Goal

新增一个 `market_maker` 做市策略骨架，先接入现有 `Strategy` 抽象、启动路由和事件循环，但不计算报价、不发订单、不影响现有策略。

## Scope

第一版只做策略骨架：

- 从 market.db 中已选中的 liquidity reward pool 读取 token 来源。
- 当 selected pool 为空时不启动做市策略。
- 当 selected pool 非空时构建 `MarketMakerStrategy` 并注册相关 token/topic。
- 策略任务可以接收行情、仓位、订单状态、成交确认和 pool removal 事件。
- 策略第一版只空跑，不发送任何 `OrderSignal`。

第一版不做：

- 不计算 bid/ask。
- 不发下单、撤单、改单信号。
- 不读取账户余额或预算。
- 不接入风控暂停/恢复。
- 不新增配置开关。
- 不改变 `liquidity_reward` 策略行为。

## Architecture

新增 `src/strategies/market_maker.rs`，定义 `MarketMakerStrategy` 和轻量规则结构。构建入口为 `MarketMakerStrategy::from_pool_entries(Vec<ActiveRewardMarketPoolEntry>) -> anyhow::Result<Option<Self>>`：空 pool 返回 `None`，非空 pool 返回可注册、可启动的策略实例。

`MarketMakerStrategy` 实现现有 `Strategy` trait：

- `name()` 返回 `market_maker`。
- `registration()` 返回从 DB pool token 构建的 `StrategyRegistration`。
- `spawn()` 创建事件循环，消费 `StrategyEvent`，第一版只过滤相关事件并空跑，不发 `OrderSignal`。

`src/main.rs` 启动流程增加可选 market maker：

1. `build_strategies()` 调用 `market_store.load_liquidity_reward_pool_entries()`。
2. 用 selected pool 构建 `market_maker: Option<MarketMakerStrategy>`。
3. `build_strategy_registrations()` 将 market maker registration 加入 dispatcher 路由。
4. `spawn_strategy_tasks()` 为 market maker 创建独立 channel，加入 `StrategyHandle`，启动策略任务。

## Data Flow

启动时，程序先初始化 store，再构建策略。market maker 默认常开，但是否真正启动取决于 DB selected pool：

1. DB selected pool 为空：`market_maker = None`，不加入订阅、不启动任务。
2. DB selected pool 非空：根据 pool entry 的 `token1/token2` 构建注册信息。
3. dispatcher 根据 registration 分发相关 token 的行情、仓位和订单事件。
4. market maker 事件循环消费事件，但第一版不产生任何交易动作。

## Testing

按 TDD 实现以下测试：

- `MarketMakerStrategy::from_pool_entries` 在空 pool 时返回 `None`。
- `MarketMakerStrategy::from_pool_entries` 在非空 pool 时返回 `Some`，且 registration name 为 `market_maker`。
- registration 的 `related_tokens` 包含 pool entry 的 token1/token2，并去重排序。
- registration 的 `topic_tokens` 能让现有 `merge_topic_tokens` / `build_token_topics` 正常路由。
- `build_strategy_registrations` 在 market maker 存在时包含它，不存在时不包含它。

不测试下单行为，因为第一版明确不发送订单信号。

## Safety

该骨架不产生 `OrderSignal`，因此不会下单、撤单或改单。它只增加一个可选策略消费者和订阅路由；DB 没有 selected pool 时不会启动，避免空任务造成误判。