// Polymarket Liquidity Rewards Calculator
// Based on: https://docs.polymarket.com/cn/market-makers/liquidity-rewards

#[derive(Debug, Clone)]
pub struct Order {
    pub side: Side,
    pub market: Market,
    pub price: f64, // 0.0 ~ 1.0
    pub size: f64,  // in shares
}

#[derive(Debug, Clone, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Market {
    Yes,
    No,
}

#[derive(Debug)]
pub struct MarketParams {
    pub mid_price: f64,
    pub max_spread_cents: f64,
    pub scale_factor: f64,
    pub reward_pool: f64,
}

#[derive(Debug)]
pub struct QminResult {
    pub qone: f64,
    pub qtwo: f64,
    pub qmin: f64,
}

#[derive(Debug)]
pub struct RewardEstimate {
    pub my_qone: f64,
    pub my_qtwo: f64,
    pub my_qmin: f64,
    pub competitors_qmin: f64,
    pub total_qmin: f64,
    pub share: f64,
    pub estimated_reward: f64,
}

// S(v, s) = ((v - s) / v)^2 * b
fn score(max_spread: f64, spread: f64, multiplier: f64) -> f64 {
    if spread < 0.0 || spread >= max_spread {
        return 0.0;
    }
    ((max_spread - spread) / max_spread).powi(2) * multiplier
}

fn to_yes_price(order: &Order) -> f64 {
    match order.market {
        Market::Yes => order.price,
        Market::No => 1.0 - order.price,
    }
}

// YES buy OR NO sell => bid on YES
fn is_bid_on_yes(order: &Order) -> bool {
    matches!(
        (&order.side, &order.market),
        (Side::Buy, Market::Yes) | (Side::Sell, Market::No)
    )
}

// YES sell OR NO buy => ask on YES
fn is_ask_on_yes(order: &Order) -> bool {
    matches!(
        (&order.side, &order.market),
        (Side::Sell, Market::Yes) | (Side::Buy, Market::No)
    )
}

pub fn compute_q_sides(orders: &[Order], params: &MarketParams) -> (f64, f64) {
    let mid = params.mid_price;
    let v = params.max_spread_cents;

    let mut qone = 0.0_f64;
    let mut qtwo = 0.0_f64;

    for order in orders {
        let yes_price = to_yes_price(order);

        if is_bid_on_yes(order) {
            let spread = (mid - yes_price) * 100.0;
            qone += score(v, spread, 1.0) * order.size;
        }

        if is_ask_on_yes(order) {
            let spread = (yes_price - mid) * 100.0;
            qtwo += score(v, spread, 1.0) * order.size;
        }
    }

    (qone, qtwo)
}

pub fn compute_qmin(qone: f64, qtwo: f64, params: &MarketParams) -> QminResult {
    let mid = params.mid_price;
    let c = params.scale_factor;

    let is_extreme = mid < 0.10 || mid > 0.90;

    let qmin = if is_extreme {
        qone.min(qtwo)
    } else {
        let two_sided = qone.min(qtwo);
        let single_sided = (qone / c).max(qtwo / c);
        two_sided.max(single_sided)
    };

    QminResult { qone, qtwo, qmin }
}

pub fn estimate_reward(
    my_orders: &[Order],
    competitor_orders: &[Order],
    params: &MarketParams,
) -> RewardEstimate {
    let (my_qone, my_qtwo) = compute_q_sides(my_orders, params);
    let my_result = compute_qmin(my_qone, my_qtwo, params);

    let (comp_qone, comp_qtwo) = compute_q_sides(competitor_orders, params);
    let comp_result = compute_qmin(comp_qone, comp_qtwo, params);

    let my_qmin = my_result.qmin;
    let competitors_qmin = comp_result.qmin;
    let total_qmin = my_qmin + competitors_qmin;

    let share = if total_qmin > 0.0 {
        my_qmin / total_qmin
    } else {
        0.0
    };

    RewardEstimate {
        my_qone,
        my_qtwo,
        my_qmin,
        competitors_qmin,
        total_qmin,
        share,
        estimated_reward: share * params.reward_pool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_params() -> MarketParams {
        MarketParams {
            mid_price: 0.50,
            max_spread_cents: 3.0,
            scale_factor: 3.0,
            reward_pool: 500.0,
        }
    }

    #[test]
    fn test_score_at_mid() {
        assert!((score(3.0, 0.0, 1.0) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_score_at_max_spread() {
        assert_eq!(score(3.0, 3.0, 1.0), 0.0);
    }

    #[test]
    fn test_score_outside_spread() {
        assert_eq!(score(3.0, 4.0, 1.0), 0.0);
        assert_eq!(score(3.0, -0.1, 1.0), 0.0);
    }

    #[test]
    fn test_score_midpoint() {
        let s = score(3.0, 1.5, 1.0);
        assert!((s - 0.25).abs() < 1e-9);
    }

    #[test]
    fn test_two_sided_qmin() {
        let params = default_params();
        let orders = vec![
            Order {
                side: Side::Buy,
                market: Market::Yes,
                price: 0.49,
                size: 100.0,
            },
            Order {
                side: Side::Sell,
                market: Market::Yes,
                price: 0.51,
                size: 100.0,
            },
        ];
        let (qone, qtwo) = compute_q_sides(&orders, &params);
        assert!(
            (qone - qtwo).abs() < 1e-9,
            "balanced orders should have equal qone/qtwo"
        );
        let r = compute_qmin(qone, qtwo, &params);
        assert!((r.qmin - qone).abs() < 1e-9);
    }

    #[test]
    fn test_single_sided_penalty() {
        let params = default_params();
        let orders = vec![Order {
            side: Side::Buy,
            market: Market::Yes,
            price: 0.49,
            size: 100.0,
        }];
        let (qone, qtwo) = compute_q_sides(&orders, &params);
        assert_eq!(qtwo, 0.0);
        let r = compute_qmin(qone, qtwo, &params);
        assert!((r.qmin - qone / params.scale_factor).abs() < 1e-9);
    }

    #[test]
    fn test_extreme_price_requires_two_sided() {
        let mut params = default_params();
        params.mid_price = 0.05;
        let orders = vec![Order {
            side: Side::Buy,
            market: Market::Yes,
            price: 0.04,
            size: 100.0,
        }];
        let (qone, qtwo) = compute_q_sides(&orders, &params);
        let r = compute_qmin(qone, qtwo, &params);
        assert_eq!(r.qmin, 0.0);
    }

    #[test]
    fn test_no_market_conversion() {
        let params = default_params();
        let no_sell = vec![Order {
            side: Side::Sell,
            market: Market::No,
            price: 0.51,
            size: 100.0,
        }];
        let yes_buy = vec![Order {
            side: Side::Buy,
            market: Market::Yes,
            price: 0.49,
            size: 100.0,
        }];
        let (q1, _) = compute_q_sides(&no_sell, &params);
        let (q2, _) = compute_q_sides(&yes_buy, &params);
        assert!(
            (q1 - q2).abs() < 1e-9,
            "NO sell and YES buy should produce identical scores"
        );
    }

    #[test]
    fn test_full_reward_estimate() {
        let params = default_params();
        let my_orders = vec![
            Order {
                side: Side::Buy,
                market: Market::Yes,
                price: 0.49,
                size: 100.0,
            },
            Order {
                side: Side::Sell,
                market: Market::Yes,
                price: 0.51,
                size: 100.0,
            },
        ];
        let est = estimate_reward(&my_orders, &[], &params);
        assert!(
            (est.share - 1.0).abs() < 1e-9,
            "100% share with no competitors"
        );
        assert!((est.estimated_reward - params.reward_pool).abs() < 1e-6);
    }
}
