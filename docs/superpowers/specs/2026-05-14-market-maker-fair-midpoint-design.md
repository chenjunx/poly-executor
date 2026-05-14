# Market Maker Fair Midpoint Design

## Goal

在 `market_maker` 策略中新增 Fair Midpoint 纯计算函数，用清洗后的盘口最优买卖价和最优档数量计算整数 tick fair mid，为后续报价逻辑提供基础，但本次不改变策略事件循环行为。

## Scope

本次只做计算函数和单元测试：

- 新增 `compute_fair_midpoint(book: &CleanOrderbook) -> u16`。
- 输入使用 `CleanOrderbook.best_bid_price`、`best_ask_price`、`best_bid_size`、`best_ask_size`。
- 返回类型为整数 tick `u16`，与现有盘口价格字段一致。
- microprice 结果四舍五入到最近 tick。
- 最终结果限制在 `[best_bid_price, best_ask_price]` 区间内。

本次不做：

- 不在事件循环中调用该函数。
- 不缓存 token 的 fair mid。
- 不生成 bid/ask 报价。
- 不发送任何 `OrderSignal`。
- 不新增配置开关。

## Formula

令：

- `bid = best_bid_price`
- `ask = best_ask_price`
- `bid_size = best_bid_size`
- `ask_size = best_ask_size`

当 `bid_size + ask_size > 0` 时：

```text
microprice = (ask * bid_size + bid * ask_size) / (bid_size + ask_size)
```

当 `bid_size + ask_size == 0` 时：

```text
microprice = (bid + ask) / 2
```

最后：

```text
fair_mid = clamp(round(microprice), bid, ask)
```

## Architecture

函数放在 `src/strategies/market_maker.rs`，与 `MarketMakerStrategy` 同模块。它只依赖 `CleanOrderbook`，不读取策略状态，不访问 DB，不发送订单，因此可用普通单元测试覆盖。

第一版保持 `MarketMakerStrategy::spawn` 空跑语义不变。后续报价逻辑可以在处理 `StrategyEvent::Market` 时复用该函数，但不在本次实现中提前引入状态。

## Testing

按 TDD 增加单元测试：

- 双边最优档数量相等时返回简单中点 tick。
- bid 侧数量更大时结果向 ask 侧偏移。
- ask 侧数量更大时结果向 bid 侧偏移。
- 两边最优档数量都为 0 时回退简单中点。
- 极端权重下结果仍限制在 `[bid, ask]`。

## Safety

该改动只新增纯计算函数和测试，不改变运行时事件循环，不发订单，不触发撤单/改单，不读写数据库。