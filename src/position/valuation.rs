use polymarket_client_sdk_v2::types::Decimal;

use crate::market::MarketBookReadHandle;
use crate::position_engine::{PositionEntryKey, PositionEntrySnapshot, PositionReadHandle};
use crate::strategy::CleanOrderbook;

const PRICE_SCALE: u32 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkPriceKind {
    BestBid,
    Midpoint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenValuation {
    pub token_id: String,
    pub filled_position: Decimal,
    pub cost_basis: Decimal,
    pub realized_pnl: Decimal,
    pub mark_price: Decimal,
    pub market_value: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub last_update_seq: u64,
    pub last_update_ts_ms: u64,
    pub degraded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioValuation {
    pub market_value: Decimal,
    pub cost_basis: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub tokens: Vec<TokenValuation>,
    pub missing_price_tokens: Vec<String>,
    pub degraded: bool,
}

pub trait MarkPriceReader: Send + Sync {
    fn mark_price(&self, token_id: &str) -> Option<Decimal>;
}

#[derive(Clone)]
pub struct MarketBookMarkPriceReader {
    books: MarketBookReadHandle,
    kind: MarkPriceKind,
}

impl MarketBookMarkPriceReader {
    pub fn new(books: MarketBookReadHandle, kind: MarkPriceKind) -> Self {
        Self { books, kind }
    }
}

impl MarkPriceReader for MarketBookMarkPriceReader {
    fn mark_price(&self, token_id: &str) -> Option<Decimal> {
        self.books
            .get(token_id)
            .and_then(|book| mark_price_from_book(book.as_ref(), self.kind))
    }
}

pub fn mark_price_from_book(book: &CleanOrderbook, kind: MarkPriceKind) -> Option<Decimal> {
    match kind {
        MarkPriceKind::BestBid => Some(price_to_decimal(book.best_bid_price)),
        MarkPriceKind::Midpoint => Some(
            (price_to_decimal(book.best_bid_price) + price_to_decimal(book.best_ask_price))
                / Decimal::from(2),
        ),
    }
}

pub fn value_token(
    token_id: impl Into<String>,
    position: &PositionEntrySnapshot,
    mark_price: Decimal,
) -> TokenValuation {
    let market_value = position.filled_position * mark_price;
    let unrealized_pnl = market_value - position.cost_basis;
    let total_pnl = position.realized_pnl + unrealized_pnl;

    TokenValuation {
        token_id: token_id.into(),
        filled_position: position.filled_position,
        cost_basis: position.cost_basis,
        realized_pnl: position.realized_pnl,
        mark_price,
        market_value,
        unrealized_pnl,
        total_pnl,
        last_update_seq: position.last_update_seq,
        last_update_ts_ms: position.last_update_ts_ms,
        degraded: position.degraded,
    }
}

pub fn value_portfolio_from_tokens(tokens: Vec<TokenValuation>) -> PortfolioValuation {
    let mut market_value = Decimal::ZERO;
    let mut cost_basis = Decimal::ZERO;
    let mut realized_pnl = Decimal::ZERO;
    let mut unrealized_pnl = Decimal::ZERO;
    let mut total_pnl = Decimal::ZERO;
    let mut degraded = false;

    for token in &tokens {
        market_value += token.market_value;
        cost_basis += token.cost_basis;
        realized_pnl += token.realized_pnl;
        unrealized_pnl += token.unrealized_pnl;
        total_pnl += token.total_pnl;
        degraded |= token.degraded;
    }

    PortfolioValuation {
        market_value,
        cost_basis,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        tokens,
        missing_price_tokens: Vec::new(),
        degraded,
    }
}

pub fn value_global_portfolio<R: MarkPriceReader>(
    position_read: &PositionReadHandle,
    mark_prices: &R,
) -> PortfolioValuation {
    let mut tokens = Vec::new();
    let mut missing_price_tokens = Vec::new();

    for (key, position) in position_read.snapshot_all_weak() {
        let PositionEntryKey::Global { token_id } = key else {
            continue;
        };
        if position.filled_position == Decimal::ZERO && position.cost_basis == Decimal::ZERO {
            continue;
        }
        match mark_prices.mark_price(&token_id) {
            Some(mark_price) => tokens.push(value_token(token_id, &position, mark_price)),
            None => missing_price_tokens.push(token_id),
        }
    }

    missing_price_tokens.sort();
    let mut portfolio = value_portfolio_from_tokens(tokens);
    portfolio.degraded |= !missing_price_tokens.is_empty();
    portfolio.missing_price_tokens = missing_price_tokens;
    portfolio
}

fn price_to_decimal(price: u16) -> Decimal {
    Decimal::from(price) / Decimal::from(PRICE_SCALE)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use super::*;
    use crate::position_engine::{
        PositionEvent, PositionEventSource, PositionKeeper, PositionSide,
    };

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).expect("decimal should build")
    }

    fn position(filled_position: f64, cost_basis: f64, realized_pnl: f64) -> PositionEntrySnapshot {
        PositionEntrySnapshot {
            filled_position: dec(filled_position),
            cost_basis: dec(cost_basis),
            realized_pnl: dec(realized_pnl),
            working_buy_exposure: Decimal::ZERO,
            working_sell_exposure: Decimal::ZERO,
            last_update_seq: 7,
            last_update_ts_ms: 1000,
            degraded: false,
        }
    }

    #[derive(Default)]
    struct TestMarkPrices {
        prices: HashMap<String, Decimal>,
    }

    impl TestMarkPrices {
        fn with(mut self, token_id: &str, price: f64) -> Self {
            self.prices.insert(token_id.to_string(), dec(price));
            self
        }
    }

    impl MarkPriceReader for TestMarkPrices {
        fn mark_price(&self, token_id: &str) -> Option<Decimal> {
            self.prices.get(token_id).copied()
        }
    }

    #[test]
    fn token_valuation_marks_position_to_market_price() {
        let valuation = value_token("token-1", &position(100.0, 40.0, 1.5), dec(0.45));

        assert_eq!(valuation.market_value, dec(45.0));
        assert_eq!(valuation.unrealized_pnl, dec(5.0));
        assert_eq!(valuation.total_pnl, dec(6.5));
        assert_eq!(valuation.last_update_seq, 7);
    }

    #[test]
    fn portfolio_valuation_sums_token_values() {
        let tokens = vec![
            value_token("yes", &position(100.0, 40.0, 0.0), dec(0.45)),
            value_token("no", &position(50.0, 30.0, 2.0), dec(0.50)),
        ];

        let valuation = value_portfolio_from_tokens(tokens);

        assert_eq!(valuation.market_value, dec(70.0));
        assert_eq!(valuation.cost_basis, dec(70.0));
        assert_eq!(valuation.realized_pnl, dec(2.0));
        assert_eq!(valuation.unrealized_pnl, Decimal::ZERO);
        assert_eq!(valuation.total_pnl, dec(2.0));
        assert!(valuation.missing_price_tokens.is_empty());
    }

    #[test]
    fn global_portfolio_uses_only_global_position_entries() {
        let mut keeper = PositionKeeper::default();
        keeper.apply_event(PositionEvent::OrderWorkingRegistered {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "buy-1".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            price: dec(0.40),
            size: dec(100.0),
            seq: 1,
            ts_ms: 100,
            source: PositionEventSource::Live,
            recovery: false,
        });
        keeper.apply_event(PositionEvent::OrderFillApplied {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "buy-1".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            fill_qty: dec(100.0),
            fill_price: dec(0.40),
            cum_qty: Some(dec(100.0)),
            seq: 2,
            ts_ms: 101,
            source: PositionEventSource::Live,
            recovery: false,
        });
        let publisher = crate::position_engine::PositionSnapshotPublisher::default();
        publisher.publish_changed(
            &keeper,
            &[
                PositionEntryKey::Strategy {
                    strategy_id: "strategy-a".to_string(),
                    token_id: "token-1".to_string(),
                },
                PositionEntryKey::Global {
                    token_id: "token-1".to_string(),
                },
            ],
        );
        let prices = TestMarkPrices::default().with("token-1", 0.45);

        let valuation = value_global_portfolio(&publisher.read_handle(), &prices);

        assert_eq!(valuation.tokens.len(), 1);
        assert_eq!(valuation.tokens[0].token_id, "token-1");
        assert_eq!(valuation.market_value, dec(45.0));
        assert_eq!(valuation.unrealized_pnl, dec(5.0));
    }

    #[test]
    fn global_portfolio_reports_missing_prices_and_marks_degraded() {
        let publisher = crate::position_engine::PositionSnapshotPublisher::default();
        let mut keeper = PositionKeeper::default();
        keeper.apply_event(PositionEvent::OrderWorkingRegistered {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "buy-1".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            price: dec(0.40),
            size: dec(100.0),
            seq: 1,
            ts_ms: 100,
            source: PositionEventSource::Live,
            recovery: false,
        });
        keeper.apply_event(PositionEvent::OrderFillApplied {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "buy-1".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            fill_qty: dec(100.0),
            fill_price: dec(0.40),
            cum_qty: Some(dec(100.0)),
            seq: 2,
            ts_ms: 101,
            source: PositionEventSource::Live,
            recovery: false,
        });
        publisher.publish_changed(
            &keeper,
            &[PositionEntryKey::Global {
                token_id: "token-1".to_string(),
            }],
        );

        let valuation =
            value_global_portfolio(&publisher.read_handle(), &TestMarkPrices::default());

        assert_eq!(valuation.tokens.len(), 0);
        assert_eq!(valuation.missing_price_tokens, vec!["token-1".to_string()]);
        assert!(valuation.degraded);
    }

    #[test]
    fn market_book_mark_price_reader_supports_best_bid_and_midpoint() {
        let book = CleanOrderbook {
            best_bid_price: 4000,
            best_bid_size: 100,
            best_ask_price: 5000,
            best_ask_size: 200,
            timestamp_ms: 1,
            bids: Arc::new(BTreeMap::from([(4000, 100)])),
            asks: Arc::new(BTreeMap::from([(5000, 200)])),
        };

        assert_eq!(
            mark_price_from_book(&book, MarkPriceKind::BestBid),
            Some(dec(0.4))
        );
        assert_eq!(
            mark_price_from_book(&book, MarkPriceKind::Midpoint),
            Some(dec(0.45))
        );
    }
}
