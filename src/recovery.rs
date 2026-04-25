use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use polymarket_client_sdk::clob::types::request::{OrdersRequest, TradesRequest};
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk::error::{Kind as PmErrorKind, Status as PmStatus, StatusCode};
use polymarket_client_sdk::types::Decimal;

use crate::clob_client::{AuthenticatedClobClient, build_authenticated_clob_client};
use crate::config::AuthConfig;
use crate::storage::{
    OrderStore, StoredMidRequoteSharedState, StoredMidRequoteSideState, StoredOrder,
};
use crate::strategies::mid_requote::{MidRequoteRestoreSideState, MidRequoteRestoreState};
use crate::strategy::{OrderCorrelationMap, QuoteSide};

const END_CURSOR: &str = "LTE=";

pub struct RecoveryCoordinator {
    order_store: OrderStore,
    auth: AuthConfig,
    simulation_enabled: bool,
}

pub struct RecoveryArtifacts {
    pub order_correlations: OrderCorrelationMap,
    pub mid_requote_restore_states: HashMap<String, MidRequoteRestoreState>,
}

impl RecoveryCoordinator {
    pub fn new(order_store: OrderStore, auth: AuthConfig, simulation_enabled: bool) -> Self {
        Self {
            order_store,
            auth,
            simulation_enabled,
        }
    }

    pub async fn recover(&self) -> anyhow::Result<RecoveryArtifacts> {
        let local_projection = self.load_local_projection()?;
        let reconciled_active_orders = self
            .reconcile_orders_against_exchange(local_projection.active_orders)
            .await?;
        let order_correlations = self.build_order_correlations(&reconciled_active_orders)?;
        let mid_requote_restore_states = build_mid_requote_restore_states(
            local_projection.shared_states,
            local_projection.side_states,
            &order_correlations,
        );

        Ok(RecoveryArtifacts {
            order_correlations,
            mid_requote_restore_states,
        })
    }

    fn load_local_projection(&self) -> anyhow::Result<LocalProjection> {
        Ok(LocalProjection {
            active_orders: self.order_store.load_active_orders()?,
            shared_states: self.order_store.load_mid_requote_shared_states()?,
            side_states: self.order_store.load_mid_requote_side_states()?,
        })
    }

    async fn reconcile_orders_against_exchange(
        &self,
        stored_orders: Vec<StoredOrder>,
    ) -> anyhow::Result<Vec<StoredOrder>> {
        if self.simulation_enabled {
            return Ok(stored_orders);
        }

        let client = build_authenticated_clob_client(&self.auth).await?;
        let remote_open_orders = fetch_remote_open_orders(&client).await?;
        let mut reconciled = Vec::new();

        for stored_order in stored_orders {
            if let Some(updated) = self
                .reconcile_stored_order(&client, &remote_open_orders, stored_order)
                .await?
            {
                reconciled.push(updated);
            }
        }

        Ok(reconciled)
    }

    async fn reconcile_stored_order(
        &self,
        client: &AuthenticatedClobClient,
        remote_open_orders: &HashMap<String, OpenOrderResponse>,
        stored_order: StoredOrder,
    ) -> anyhow::Result<Option<StoredOrder>> {
        let Some(remote_order_id) = stored_order.remote_order_id.as_deref() else {
            self.mark_missing_remote_order_id_unknown(&stored_order)?;
            return Ok(None);
        };

        if let Some(open_order) = remote_open_orders.get(remote_order_id) {
            return self
                .reconcile_present_open_order(&stored_order, remote_order_id, open_order)
                .map(Some);
        }

        match client.order(remote_order_id).await {
            Ok(order) => {
                self.reconcile_resolved_order_lookup(&stored_order, remote_order_id, &order)
            }
            Err(error) if is_not_found_status(&error) => {
                self.reconcile_missing_remote_order(client, &stored_order, remote_order_id)
                    .await?;
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn mark_missing_remote_order_id_unknown(
        &self,
        stored_order: &StoredOrder,
    ) -> anyhow::Result<()> {
        self.order_store
            .update_order_status_by_local(&stored_order.local_order_id, "unknown")?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            None,
            "startup_reconciled",
            serde_json::json!({
                "result": "missing_remote_order_id",
                "status": "unknown",
            }),
        )?;
        Ok(())
    }

    fn reconcile_present_open_order(
        &self,
        stored_order: &StoredOrder,
        remote_order_id: &str,
        open_order: &OpenOrderResponse,
    ) -> anyhow::Result<StoredOrder> {
        let status = map_open_order_status(open_order);
        self.order_store
            .update_order_status_by_remote(remote_order_id, status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "present_in_open_orders",
                "status": status,
                "original_size": open_order.original_size.to_string(),
                "size_matched": open_order.size_matched.to_string(),
                "price": open_order.price.to_string(),
            }),
        )?;

        let mut updated = stored_order.clone();
        updated.status = status.to_string();
        Ok(updated)
    }

    fn reconcile_resolved_order_lookup(
        &self,
        stored_order: &StoredOrder,
        remote_order_id: &str,
        order: &OpenOrderResponse,
    ) -> anyhow::Result<Option<StoredOrder>> {
        let status = map_single_order_status(order);
        self.order_store
            .update_order_status_by_remote(remote_order_id, status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "resolved_by_order_lookup",
                "status": status,
                "original_size": order.original_size.to_string(),
                "size_matched": order.size_matched.to_string(),
                "price": order.price.to_string(),
            }),
        )?;

        if is_terminal_or_unrecoverable_status(status) {
            return Ok(None);
        }

        let mut updated = stored_order.clone();
        updated.status = status.to_string();
        Ok(Some(updated))
    }

    async fn reconcile_missing_remote_order(
        &self,
        client: &AuthenticatedClobClient,
        stored_order: &StoredOrder,
        remote_order_id: &str,
    ) -> anyhow::Result<()> {
        let terminal_status = infer_missing_remote_terminal_status(client, stored_order).await?;
        self.order_store
            .update_order_status_by_local(&stored_order.local_order_id, terminal_status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "missing_from_remote",
                "status": terminal_status,
            }),
        )?;
        Ok(())
    }

    fn build_order_correlations(
        &self,
        reconciled_active_orders: &[StoredOrder],
    ) -> anyhow::Result<OrderCorrelationMap> {
        let order_correlations: OrderCorrelationMap = Arc::new(DashMap::new());

        for stored_order in reconciled_active_orders {
            let local_meta = stored_order.to_local_order_meta();
            order_correlations.insert(local_meta.local_order_id.clone(), local_meta.clone());
            if let Some(remote_order_id) = &local_meta.remote_order_id {
                order_correlations.insert(remote_order_id.clone(), local_meta.clone());
            }
            self.order_store.append_order_event(
                Some(&local_meta.local_order_id),
                local_meta.remote_order_id.as_deref(),
                "recovered_on_startup",
                serde_json::json!({
                    "strategy": local_meta.strategy.as_ref(),
                    "topic": local_meta.topic.as_ref().map(|topic| topic.as_ref()),
                    "token": local_meta.token,
                    "side": format!("{:?}", local_meta.side),
                    "price": local_meta.price.to_string(),
                    "order_size": local_meta.order_size.to_string(),
                    "status": stored_order.status,
                    "last_mid": stored_order.last_mid.map(|value| value.to_string()),
                }),
            )?;
        }

        Ok(order_correlations)
    }
}

struct LocalProjection {
    active_orders: Vec<StoredOrder>,
    shared_states: Vec<StoredMidRequoteSharedState>,
    side_states: Vec<StoredMidRequoteSideState>,
}

async fn fetch_remote_open_orders(
    client: &AuthenticatedClobClient,
) -> anyhow::Result<HashMap<String, OpenOrderResponse>> {
    let mut remote_open_orders = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client
            .orders(&OrdersRequest::default(), cursor.clone())
            .await?;
        for order in page.data {
            remote_open_orders.insert(order.id.clone(), order);
        }
        if page.next_cursor == END_CURSOR {
            break;
        }
        cursor = Some(page.next_cursor);
    }

    Ok(remote_open_orders)
}

fn build_mid_requote_restore_states(
    shared_states: Vec<StoredMidRequoteSharedState>,
    side_states: Vec<StoredMidRequoteSideState>,
    order_correlations: &OrderCorrelationMap,
) -> HashMap<String, MidRequoteRestoreState> {
    let mut restored_mid_requote_states = restore_states_from_shared_states(shared_states);

    for side_state in side_states {
        let restore_state = restored_mid_requote_states
            .entry(side_state.token.clone())
            .or_insert_with(default_mid_requote_restore_state);
        apply_mid_requote_side_state(restore_state, side_state, order_correlations);
    }

    restored_mid_requote_states
}

fn restore_states_from_shared_states(
    shared_states: Vec<StoredMidRequoteSharedState>,
) -> HashMap<String, MidRequoteRestoreState> {
    shared_states
        .into_iter()
        .map(|state| {
            (
                state.token.clone(),
                MidRequoteRestoreState {
                    topic: Arc::from(state.topic),
                    buy: MidRequoteRestoreSideState::default(),
                    sell: MidRequoteRestoreSideState::default(),
                    last_mid: state.last_mid,
                    last_best_bid: state.last_best_bid,
                    last_best_ask: state.last_best_ask,
                    last_position_size: state.last_position_size,
                },
            )
        })
        .collect()
}

fn default_mid_requote_restore_state() -> MidRequoteRestoreState {
    MidRequoteRestoreState {
        topic: Arc::from("mid"),
        buy: MidRequoteRestoreSideState::default(),
        sell: MidRequoteRestoreSideState::default(),
        last_mid: None,
        last_best_bid: None,
        last_best_ask: None,
        last_position_size: Decimal::ZERO,
    }
}

fn apply_mid_requote_side_state(
    restore_state: &mut MidRequoteRestoreState,
    side_state: StoredMidRequoteSideState,
    order_correlations: &OrderCorrelationMap,
) {
    let side = side_state.side;
    let lane = restore_lane_mut(restore_state, side);

    lane.active_local_order_id = side_state
        .active_local_order_id
        .filter(|order_id| correlated_order_matches_side(order_correlations, order_id, side));
    lane.active_order_size = lane.active_local_order_id.as_ref().and_then(|order_id| {
        order_correlations
            .get(order_id)
            .map(|entry| entry.order_size)
    });

    lane.pending_local_order_id = side_state
        .pending_local_order_id
        .filter(|order_id| correlated_order_matches_side(order_correlations, order_id, side));
    apply_pending_replacement_fields(
        lane,
        side_state.pending_price,
        side_state.pending_order_size,
        side_state.pending_mid,
    );
    lane.last_quoted_mid = side_state.last_quoted_mid;
    lane.cancel_requested = side_state.cancel_requested;
}

fn restore_lane_mut(
    restore_state: &mut MidRequoteRestoreState,
    side: QuoteSide,
) -> &mut MidRequoteRestoreSideState {
    match side {
        QuoteSide::Buy => &mut restore_state.buy,
        QuoteSide::Sell => &mut restore_state.sell,
    }
}

fn correlated_order_matches_side(
    order_correlations: &OrderCorrelationMap,
    order_id: &str,
    side: QuoteSide,
) -> bool {
    order_correlations
        .get(order_id)
        .is_some_and(|entry| entry.side == side)
}

fn apply_pending_replacement_fields(
    lane: &mut MidRequoteRestoreSideState,
    pending_price: Option<Decimal>,
    pending_order_size: Option<Decimal>,
    pending_mid: Option<Decimal>,
) {
    if lane.pending_local_order_id.is_some() {
        lane.pending_price = pending_price;
        lane.pending_order_size = pending_order_size;
        lane.pending_mid = pending_mid;
    } else {
        lane.pending_price = None;
        lane.pending_order_size = None;
        lane.pending_mid = None;
    }
}

fn map_open_order_status(order: &OpenOrderResponse) -> &'static str {
    if order.size_matched == Decimal::ZERO {
        "open"
    } else {
        "partially_filled"
    }
}

fn map_single_order_status(order: &OpenOrderResponse) -> &'static str {
    use polymarket_client_sdk::clob::types::OrderStatusType;

    match order.status {
        OrderStatusType::Canceled => "canceled",
        OrderStatusType::Matched => {
            if order.size_matched == order.original_size {
                "filled"
            } else {
                "partially_filled"
            }
        }
        OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Unmatched => {
            map_open_order_status(order)
        }
        OrderStatusType::Unknown => "unknown",
        _ => "unknown",
    }
}

fn is_terminal_or_unrecoverable_status(status: &str) -> bool {
    matches!(
        status,
        "filled" | "canceled" | "rejected" | "failed" | "unknown"
    )
}

async fn infer_missing_remote_terminal_status(
    client: &AuthenticatedClobClient,
    stored_order: &StoredOrder,
) -> anyhow::Result<&'static str> {
    let request = TradesRequest::builder()
        .asset_id(stored_order.token.clone())
        .build();
    let page = client.trades(&request, None).await?;
    let has_trade_for_order = page.data.iter().any(|trade| {
        trade.maker_orders.iter().any(|maker_order| {
            maker_order.order_id == stored_order.remote_order_id.clone().unwrap_or_default()
        })
    });
    Ok(if has_trade_for_order {
        "filled"
    } else {
        "unknown"
    })
}

fn is_not_found_status(error: &polymarket_client_sdk::error::Error) -> bool {
    error.kind() == PmErrorKind::Status
        && error
            .downcast_ref::<PmStatus>()
            .is_some_and(|status| status.status_code == StatusCode::NOT_FOUND)
}
