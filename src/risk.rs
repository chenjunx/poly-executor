use std::collections::HashMap;
use std::sync::Arc;

use polymarket_client_sdk_v2::types::Decimal;

use crate::order_gateway::{
    CancelOrderRequest, GatewayState, OrderRiskCheck, OrderSide, PlaceOrderRequest, RiskDecision,
    StrategyId,
};
use crate::position_engine::{PositionEngineStatus, PositionReadHandle, PositionStatusHandle};
use crate::strategy::{StrategyKind, StrategyRegistration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLayer {
    Global,
    StrategyKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketTopOfBook {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
}

pub trait MarketRiskReader: Send + Sync {
    fn best_bid_ask(&self, token_id: &str) -> Option<MarketTopOfBook>;
    fn mid_price(&self, token_id: &str) -> Option<Decimal>;
}

pub struct NoopMarketRiskReader;

impl MarketRiskReader for NoopMarketRiskReader {
    fn best_bid_ask(&self, _token_id: &str) -> Option<MarketTopOfBook> {
        None
    }

    fn mid_price(&self, _token_id: &str) -> Option<Decimal> {
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyKindOrderSizeLimit {
    pub kind: StrategyKind,
    pub max_size: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyKindTokenExposureLimit {
    pub kind: StrategyKind,
    pub max_exposure: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskConfig {
    pub max_single_order_size: Decimal,
    pub max_global_token_exposure: Decimal,
    pub default_strategy_kind_max_order_size: Decimal,
    pub default_strategy_kind_token_exposure: Decimal,
    pub strategy_kind_order_size_limits: Vec<StrategyKindOrderSizeLimit>,
    pub strategy_kind_token_exposure_limits: Vec<StrategyKindTokenExposureLimit>,
}

impl Default for RiskConfig {
    fn default() -> Self {
        let high_limit = Decimal::try_from(1_000_000_f64).expect("risk default decimal");
        Self {
            max_single_order_size: high_limit,
            max_global_token_exposure: high_limit,
            default_strategy_kind_max_order_size: high_limit,
            default_strategy_kind_token_exposure: high_limit,
            strategy_kind_order_size_limits: Vec::new(),
            strategy_kind_token_exposure_limits: Vec::new(),
        }
    }
}

impl RiskConfig {
    fn max_order_size_for(&self, kind: StrategyKind) -> Decimal {
        self.strategy_kind_order_size_limits
            .iter()
            .find(|limit| limit.kind == kind)
            .map(|limit| limit.max_size)
            .unwrap_or(self.default_strategy_kind_max_order_size)
    }

    fn max_token_exposure_for(&self, kind: StrategyKind) -> Decimal {
        self.strategy_kind_token_exposure_limits
            .iter()
            .find(|limit| limit.kind == kind)
            .map(|limit| limit.max_exposure)
            .unwrap_or(self.default_strategy_kind_token_exposure)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StrategyKindRegistry {
    by_name: HashMap<String, StrategyKind>,
}

impl StrategyKindRegistry {
    pub fn from_registrations(registrations: &[StrategyRegistration]) -> Self {
        let by_name = registrations
            .iter()
            .map(|registration| (registration.name.to_string(), registration.kind))
            .collect();
        Self { by_name }
    }

    pub fn kind_for(&self, strategy_id: &StrategyId) -> Option<StrategyKind> {
        self.by_name.get(strategy_id.as_str()).copied()
    }
}

pub struct RiskContext<'a> {
    pub gateway_state: &'a GatewayState,
    pub position_read: &'a PositionReadHandle,
    pub position_status: &'a PositionStatusHandle,
    pub strategy_registry: &'a StrategyKindRegistry,
    pub market_reader: &'a dyn MarketRiskReader,
    pub config: &'a RiskConfig,
}

pub trait RiskRule: Send + Sync {
    fn id(&self) -> &'static str;
    fn layer(&self) -> RiskLayer;
    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision;

    fn check_cancel(&self, _request: &CancelOrderRequest, _ctx: &RiskContext<'_>) -> RiskDecision {
        RiskDecision::Allow
    }
}

pub struct GatewayRiskEngine {
    config: RiskConfig,
    position_read: PositionReadHandle,
    position_status: PositionStatusHandle,
    strategy_registry: StrategyKindRegistry,
    market_reader: Arc<dyn MarketRiskReader>,
    rules: Vec<Arc<dyn RiskRule>>,
}

impl GatewayRiskEngine {
    pub fn new(
        config: RiskConfig,
        position_read: PositionReadHandle,
        position_status: PositionStatusHandle,
        strategy_registry: StrategyKindRegistry,
        market_reader: Arc<dyn MarketRiskReader>,
    ) -> Self {
        Self::with_rules(
            config,
            position_read,
            position_status,
            strategy_registry,
            market_reader,
            vec![
                Arc::new(PositionEngineHealthRule),
                Arc::new(BasicOrderSanityRule),
                Arc::new(GlobalTokenExposureRule),
                Arc::new(KnownStrategyRule),
                Arc::new(StrategyKindOrderSizeRule),
                Arc::new(StrategyKindTokenExposureRule),
            ],
        )
    }

    pub fn with_rules(
        config: RiskConfig,
        position_read: PositionReadHandle,
        position_status: PositionStatusHandle,
        strategy_registry: StrategyKindRegistry,
        market_reader: Arc<dyn MarketRiskReader>,
        rules: Vec<Arc<dyn RiskRule>>,
    ) -> Self {
        Self {
            config,
            position_read,
            position_status,
            strategy_registry,
            market_reader,
            rules,
        }
    }

    fn context<'a>(&'a self, gateway_state: &'a GatewayState) -> RiskContext<'a> {
        RiskContext {
            gateway_state,
            position_read: &self.position_read,
            position_status: &self.position_status,
            strategy_registry: &self.strategy_registry,
            market_reader: self.market_reader.as_ref(),
            config: &self.config,
        }
    }
}

impl OrderRiskCheck for GatewayRiskEngine {
    fn check_place(&self, request: &PlaceOrderRequest, state: &GatewayState) -> RiskDecision {
        let ctx = self.context(state);
        for rule in &self.rules {
            let decision = rule.check_place(request, &ctx);
            if !matches!(decision, RiskDecision::Allow) {
                return decision;
            }
        }
        RiskDecision::Allow
    }

    fn check_cancel(&self, request: &CancelOrderRequest, state: &GatewayState) -> RiskDecision {
        let ctx = self.context(state);
        for rule in &self.rules {
            let decision = rule.check_cancel(request, &ctx);
            if !matches!(decision, RiskDecision::Allow) {
                return decision;
            }
        }
        RiskDecision::Allow
    }
}

pub struct PositionEngineHealthRule;

impl RiskRule for PositionEngineHealthRule {
    fn id(&self) -> &'static str {
        "position_engine_health"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::Global
    }

    fn check_place(&self, _request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        if ctx.position_status.status() == PositionEngineStatus::Live {
            RiskDecision::Allow
        } else {
            reject(self.id(), "position engine is not live")
        }
    }
}

pub struct BasicOrderSanityRule;

impl RiskRule for BasicOrderSanityRule {
    fn id(&self) -> &'static str {
        "basic_order_sanity"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::Global
    }

    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        if request.strategy_id.as_str().is_empty() {
            return reject(self.id(), "strategy_id is empty");
        }
        if request.token_id.as_str().is_empty() {
            return reject(self.id(), "token_id is empty");
        }
        if request.local_id.as_str().is_empty() {
            return reject(self.id(), "local_id is empty");
        }
        if request.size <= Decimal::ZERO {
            return reject(self.id(), "order size must be positive");
        }
        if request.size > ctx.config.max_single_order_size {
            return reject(self.id(), "order size exceeds global single order limit");
        }
        if let Some(price) = request.price {
            if price <= Decimal::ZERO || price > Decimal::ONE {
                return reject(self.id(), "order price is outside (0, 1]");
            }
        }
        RiskDecision::Allow
    }
}

pub struct GlobalTokenExposureRule;

impl RiskRule for GlobalTokenExposureRule {
    fn id(&self) -> &'static str {
        "global_token_exposure"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::Global
    }

    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        let projected = projected_global_exposure(request, ctx);
        if decimal_abs(projected) > ctx.config.max_global_token_exposure {
            reject(self.id(), "global token exposure limit exceeded")
        } else {
            RiskDecision::Allow
        }
    }
}

pub struct KnownStrategyRule;

impl RiskRule for KnownStrategyRule {
    fn id(&self) -> &'static str {
        "known_strategy"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::StrategyKind
    }

    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        if ctx
            .strategy_registry
            .kind_for(&request.strategy_id)
            .is_some()
        {
            RiskDecision::Allow
        } else {
            reject(self.id(), "strategy_id is not registered")
        }
    }
}

pub struct StrategyKindOrderSizeRule;

impl RiskRule for StrategyKindOrderSizeRule {
    fn id(&self) -> &'static str {
        "strategy_kind_order_size"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::StrategyKind
    }

    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        let Some(kind) = ctx.strategy_registry.kind_for(&request.strategy_id) else {
            return RiskDecision::Allow;
        };
        if request.size > ctx.config.max_order_size_for(kind) {
            reject(self.id(), "strategy kind order size limit exceeded")
        } else {
            RiskDecision::Allow
        }
    }
}

pub struct StrategyKindTokenExposureRule;

impl RiskRule for StrategyKindTokenExposureRule {
    fn id(&self) -> &'static str {
        "strategy_kind_token_exposure"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::StrategyKind
    }

    fn check_place(&self, request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        let Some(kind) = ctx.strategy_registry.kind_for(&request.strategy_id) else {
            return RiskDecision::Allow;
        };
        let projected = projected_global_exposure(request, ctx);
        if decimal_abs(projected) > ctx.config.max_token_exposure_for(kind) {
            reject(self.id(), "strategy kind token exposure limit exceeded")
        } else {
            RiskDecision::Allow
        }
    }
}

fn reject(code: &'static str, reason: &'static str) -> RiskDecision {
    RiskDecision::Reject {
        code: Arc::from(code),
        reason: Arc::from(reason),
    }
}

fn projected_global_exposure(request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> Decimal {
    let current = ctx
        .position_read
        .get_global_entry(request.token_id.as_str())
        .map(|entry| entry.theoretical_net())
        .unwrap_or(Decimal::ZERO);
    match request.side {
        OrderSide::Buy => current + request.size,
        OrderSide::Sell => current - request.size,
    }
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < Decimal::ZERO { -value } else { value }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_gateway::{
        CancelScope, GatewayOrderType, GatewayPhase, LocalOrderId, MarketId, OrderEventKind,
        OrderEventPayload, OrderGateway, OrderGatewayConfig, OrderRequest, StrategyId, TimeInForce,
        TokenId,
    };
    use crate::position_engine::{
        PositionEvent, PositionEventSource, PositionIngestor, PositionKeeper, PositionSide,
        PositionSnapshotPublisher,
    };
    use crate::strategy::{StrategyKind, StrategyRegistration};

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).expect("decimal should build")
    }

    fn registration(name: &str, kind: StrategyKind) -> StrategyRegistration {
        StrategyRegistration {
            name: Arc::from(name),
            kind,
            topics: Arc::from([Arc::from("topic")]),
            topic_tokens: Arc::from([]),
            related_tokens: Arc::from(["token-1".to_string()]),
        }
    }

    fn place(strategy_id: &str, token_id: &str, size: f64) -> PlaceOrderRequest {
        PlaceOrderRequest {
            strategy_id: StrategyId::from(strategy_id),
            market_id: Some(MarketId::from("market-1")),
            token_id: TokenId::from(token_id),
            local_id: LocalOrderId::from("local-1"),
            side: OrderSide::Buy,
            order_type: GatewayOrderType::Limit {
                time_in_force: TimeInForce::Gtc,
            },
            price: Some(dec(0.42)),
            size: dec(size),
            reason: None,
        }
    }

    fn cancel(strategy_id: &str) -> CancelOrderRequest {
        CancelOrderRequest {
            strategy_id: StrategyId::from(strategy_id),
            scope: CancelScope::AllForStrategy,
            reason: None,
        }
    }

    fn engine_with_config(config: RiskConfig) -> GatewayRiskEngine {
        let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
        GatewayRiskEngine::new(
            config,
            ingest_handle.read_handle(),
            ingest_handle.status_handle(),
            StrategyKindRegistry::from_registrations(&[registration(
                "market_maker",
                StrategyKind::MarketMaker,
            )]),
            Arc::new(NoopMarketRiskReader),
        )
    }

    fn engine_with_position(config: RiskConfig, working_buy_exposure: f64) -> GatewayRiskEngine {
        let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
        let publisher = PositionSnapshotPublisher::default();
        let read_handle = publisher.read_handle();
        let mut keeper = PositionKeeper::default();
        let changed = keeper.apply_event(PositionEvent::OrderWorkingRegistered {
            strategy_id: "market_maker".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "working-1".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            price: dec(0.4),
            size: dec(working_buy_exposure),
            seq: 1,
            ts_ms: 100,
            source: PositionEventSource::Live,
            recovery: false,
        });
        publisher.publish_changed(&keeper, &changed);
        GatewayRiskEngine::new(
            config,
            read_handle,
            ingest_handle.status_handle(),
            StrategyKindRegistry::from_registrations(&[registration(
                "market_maker",
                StrategyKind::MarketMaker,
            )]),
            Arc::new(NoopMarketRiskReader),
        )
    }

    #[test]
    fn unknown_strategy_is_rejected() {
        let engine = engine_with_config(RiskConfig::default());

        let decision =
            engine.check_place(&place("unknown", "token-1", 1.0), &GatewayState::default());

        assert!(matches!(
            decision,
            RiskDecision::Reject { ref code, .. } if code.as_ref() == "known_strategy"
        ));
    }

    #[test]
    fn position_engine_non_live_rejects_place_but_allows_cancel() {
        let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
        ingest_handle.mark_degraded();
        let engine = GatewayRiskEngine::new(
            RiskConfig::default(),
            ingest_handle.read_handle(),
            ingest_handle.status_handle(),
            StrategyKindRegistry::from_registrations(&[registration(
                "market_maker",
                StrategyKind::MarketMaker,
            )]),
            Arc::new(NoopMarketRiskReader),
        );

        let place_decision = engine.check_place(
            &place("market_maker", "token-1", 1.0),
            &GatewayState::default(),
        );
        let cancel_decision =
            engine.check_cancel(&cancel("market_maker"), &GatewayState::default());

        assert!(matches!(
            place_decision,
            RiskDecision::Reject { ref code, .. } if code.as_ref() == "position_engine_health"
        ));
        assert_eq!(cancel_decision, RiskDecision::Allow);
    }

    #[test]
    fn global_token_exposure_limit_rejects_projected_exposure() {
        let config = RiskConfig {
            max_global_token_exposure: dec(10.0),
            ..RiskConfig::default()
        };
        let engine = engine_with_position(config, 6.0);

        let decision = engine.check_place(
            &place("market_maker", "token-1", 5.0),
            &GatewayState::default(),
        );

        assert!(matches!(
            decision,
            RiskDecision::Reject { ref code, .. } if code.as_ref() == "global_token_exposure"
        ));
    }

    #[test]
    fn strategy_kind_order_size_limit_rejects_oversized_order() {
        let config = RiskConfig {
            strategy_kind_order_size_limits: vec![StrategyKindOrderSizeLimit {
                kind: StrategyKind::MarketMaker,
                max_size: dec(5.0),
            }],
            ..RiskConfig::default()
        };
        let engine = engine_with_config(config);

        let decision = engine.check_place(
            &place("market_maker", "token-1", 6.0),
            &GatewayState::default(),
        );

        assert!(matches!(
            decision,
            RiskDecision::Reject { ref code, .. } if code.as_ref() == "strategy_kind_order_size"
        ));
    }

    #[test]
    fn noop_market_reader_does_not_reject_otherwise_valid_order() {
        let engine = engine_with_config(RiskConfig::default());

        let decision = engine.check_place(
            &place("market_maker", "token-1", 1.0),
            &GatewayState::default(),
        );

        assert_eq!(decision, RiskDecision::Allow);
    }

    #[tokio::test]
    async fn gateway_risk_engine_rejection_uses_local_rejected_path() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let risk = engine_with_config(RiskConfig::default());
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(risk));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("unknown"));

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(OrderRequest::Place(place("unknown", "token-1", 1.0)))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let event = subscriber
            .try_recv_relevant()
            .expect("local rejected event should publish");
        assert_eq!(event.kind, OrderEventKind::LocalRejected);
        assert!(matches!(
            event.payload,
            OrderEventPayload::LocalRejected { .. }
        ));
    }
}
