# Liquidity Reward FSM Mermaid

本文档记录 `src/strategies/liquidity_reward_fsm.rs` 当前状态机流转。后续修改 FSM 逻辑时需要同步更新本文档。

## Quote 状态机

状态含义：

- `Idle`：空闲，没有当前买单，等待下一次可报价行情。
- `Active`：已有一张做市买单在交易所。
- `CancelingWait`：正在撤当前买单，撤完后回到空闲，不补新单。
- `CancelingReplace`：正在撤当前买单，撤完后提交 pending replacement 新买单。
- `Halted`：风控停机状态，不再恢复报价，只处理撤单重试和清仓收尾。

```mermaid
stateDiagram-v2
    [*] --> Idle

    Idle --> Active: Market 可报价 / PlaceBuy 下买单
    Active --> CancelingWait: Market 不满足报价 / CancelBuy 撤单
    Active --> CancelingReplace: Market 目标价变化 / CancelBuy 撤旧单 + pending replacement

    CancelingWait --> Idle: OrderStatus canceled/rejected 撤单确认，回到空闲
    CancelingReplace --> Active: OrderStatus canceled/rejected 撤单确认 / PlaceBuy(pending) 下替换单
    CancelingWait --> Active: OrderStatus failed 撤单失败，恢复 active
    CancelingReplace --> Active: OrderStatus failed 撤单失败，恢复 active

    Active --> Halted: OrderFill buy 买单成交 / halt_pair_fsm 停整对
    Active --> Halted: OrderStatus filled 直接成交 / halt_pair_fsm 停整对
    Active --> Halted: RewardPoolRemoval 池子剔除 / halt_pair_fsm 停整对
    CancelingWait --> Halted: OrderFill buy or RewardPoolRemoval 成交或池子剔除，进入停机
    CancelingReplace --> Halted: OrderFill buy or RewardPoolRemoval 成交或池子剔除，进入停机
    Idle --> Halted: historical OrderFill buy or RewardPoolRemoval 历史成交或池子剔除，进入停机

    Halted --> Halted: Market / retry_halted_cancel if needed 停机后行情只用于补发撤单
    Halted --> Halted: cancel_failed / reopen cancel retry window 撤单失败后重新打开重试窗口
    Halted --> Halted: Positions / maybe submit pool removal unwind 根据仓位尝试池子剔除清仓
```

## Risk 状态机

状态含义：

- `Normal`：正常风险状态，没有池子剔除清仓意图。
- `PoolRemovalPendingNoPosition`：已收到池子剔除事件，但还没有可用正仓位。
- `PoolRemovalPendingWithPosition`：已确认有正仓位，需要等行情价格可用后提交清仓卖单。
- `PoolRemovalUnwindInFlight`：池子剔除清仓卖单在途，避免重复剔除事件导致重复卖出；卖单终态后回到 pending，等待下一次 Positions 确认是否仍有仓位。

```mermaid
stateDiagram-v2
    [*] --> Normal

    Normal --> PoolRemovalPendingNoPosition: RewardPoolRemoval / mark_pool_removal_unwind 标记池子剔除清仓意图
    PoolRemovalPendingNoPosition --> PoolRemovalPendingWithPosition: Positions size > 0 收到正仓位
    PoolRemovalPendingNoPosition --> Normal: Positions missing or size <= 0 无仓位，清除风险意图

    PoolRemovalPendingWithPosition --> PoolRemovalUnwindInFlight: best_bid or mid ready / MarketSell 行情可用，提交清仓卖单
    PoolRemovalPendingWithPosition --> PoolRemovalPendingWithPosition: Positions size changes before submit 提交前仓位变化，刷新数量
    PoolRemovalPendingWithPosition --> Normal: Positions missing or size <= 0 仓位消失或清零，清除风险意图

    PoolRemovalUnwindInFlight --> PoolRemovalUnwindInFlight: repeated RewardPoolRemoval ignored 清仓在途时重复池子剔除事件不重复卖
    PoolRemovalUnwindInFlight --> PoolRemovalPendingNoPosition: OrderStatus terminal 清仓卖单终态，释放 in-flight，等待下一次 Positions 确认
    PoolRemovalPendingNoPosition --> PoolRemovalPendingWithPosition: later Positions size > 0 仍有仓位，继续补卖
```

## Unwind 卖单状态机

状态含义：

- `NoPendingUnwind`：当前没有在途清仓卖单。
- `PendingUnwind`：已有一张清仓卖单提交到 executor，等待成交、取消或失败状态。

```mermaid
stateDiagram-v2
    [*] --> NoPendingUnwind

    NoPendingUnwind --> PendingUnwind: submit_unwind / MarketSell 提交清仓卖单
    PendingUnwind --> PendingUnwind: OrderFill sell / update matched_size 卖单成交回报只更新已成交数量
    PendingUnwind --> NoPendingUnwind: OrderStatus filled/open/failed / remove pending 终态或异常状态清理 pending
    PendingUnwind --> NoPendingUnwind: OrderStatus canceled/rejected and remaining <= 0 取消/拒绝但无剩余量
    PendingUnwind --> PendingUnwind: OrderStatus canceled/rejected and remaining > 0 / submit remaining unwind 有剩余量则重新提交剩余清仓
```

## 事件流

节点含义：

- `StrategyEvent::Market`：公开行情事件，驱动正常报价、停机撤单重试、池子剔除延迟清仓。
- `StrategyEvent::OrderFill`：订单成交增量事件，买单成交会触发整对停机，unwind 卖单成交只更新已卖数量。
- `StrategyEvent::RewardPoolRemoval`：奖励池剔除事件，触发整对停机并标记等待 positions 清仓。
- `StrategyEvent::Positions`：持仓快照事件，用于池子剔除后确认实际仓位并提交清仓卖单。
- `execute_effects`：把 FSM 产生的 effect 转成 `OrderSignal` 或 DB 写入。
- `persist_token_state`：把当前 quote FSM 投影写入 orders.db，供重启恢复。

```mermaid
flowchart TD
    Market[StrategyEvent::Market<br/>行情事件] --> UpdateMarket[更新 mid/bid/ask/bids<br/>刷新行情快照]
    UpdateMarket --> HaltedCheck{QuoteState Halted?<br/>是否已停机}
    HaltedCheck -->|yes 是| RetryCancel[retry_halted_cancel<br/>必要时补发停机撤单]
    RetryCancel --> PoolUnwindReady[submit_pool_removal_unwind_if_ready<br/>如果池子剔除清仓条件就绪则提交卖单]
    PoolUnwindReady --> Execute[execute_effects<br/>执行下单/撤单/清仓/持久化 effect]
    HaltedCheck -->|no 否| QuoteDecision[quote_decision<br/>计算是否挂单、撤单或替换]
    QuoteDecision --> Place[PlaceBuy<br/>提交买单]
    QuoteDecision --> Cancel[CancelBuy<br/>提交撤单]
    QuoteDecision --> Wait[Wait<br/>保持当前状态]
    Place --> Execute
    Cancel --> Execute
    Wait --> Persist[persist_token_state<br/>持久化 quote 状态]
    Execute --> Persist

    OrderFill[StrategyEvent::OrderFill<br/>成交增量事件] --> IsUnwindFill{pending_unwinds?<br/>是否是清仓卖单成交}
    IsUnwindFill -->|yes 是| UpdateMatched[update matched_size only<br/>只更新清仓已成交数量]
    IsUnwindFill -->|no 否| IsBuy{liquidity_reward buy?<br/>是否本策略买单成交}
    IsBuy -->|yes 是| PoolPending{PoolRemovalPending?<br/>是否已在池子剔除清仓流程}
    PoolPending -->|yes 是| WaitPositions[等待 Positions 统一清仓<br/>避免按 delta_size 重复卖]
    PoolPending -->|no 否| HaltPair[halt_pair_fsm + unwind delta_size<br/>整对停机并按成交增量清仓]
    HaltPair --> Execute

    RewardRemoval[StrategyEvent::RewardPoolRemoval<br/>奖励池剔除事件] --> HaltPoolPair[halt_pair_fsm PoolRemoval<br/>整对停机并撤 active 买单]
    HaltPoolPair --> MarkRisk[mark_pool_removal_unwind<br/>标记等待持仓快照清仓]
    MarkRisk --> HasPositions{latest_positions?<br/>是否已有最近持仓快照}
    HasPositions -->|yes 是| ApplyPositions[apply_pool_removal_positions<br/>按快照仓位尝试提交清仓]
    ApplyPositions --> Execute
    HasPositions -->|no 否| Execute

    Positions[StrategyEvent::Positions<br/>持仓快照事件] --> CacheSnapshot[缓存 latest_positions<br/>更新最近持仓快照]
    CacheSnapshot --> ApplyPositions
```
