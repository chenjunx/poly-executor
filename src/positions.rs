use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use polymarket_client_sdk_v2::data::{Client, types::request::PositionsRequest};
use polymarket_client_sdk_v2::types::{Address, Decimal};
use tracing::{info, warn};

use crate::{
    config::AuthConfig,
    strategy::{PositionSnapshot, PositionView, PositionsUpdateEvent, QuoteSide, StrategyEvent},
};

const PAGE_LIMIT: i32 = 500;

#[derive(Debug, Clone)]
pub struct SimulatedFillEvent {
    pub local_order_id: String,
    pub token: String,
    pub side: QuoteSide,
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionRefreshTrigger {
    Startup,
    OrderPlacement,
    OrderUpdate,
}

pub async fn run(
    auth: AuthConfig,
    mut refresh_rx: tokio::sync::mpsc::Receiver<PositionRefreshTrigger>,
    strategy_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
) {
    let address = match Address::from_str(&auth.funder) {
        Ok(address) => address,
        Err(error) => {
            warn!(error = %error, funder = %auth.funder, "positions 解析 funder 地址失败");
            return;
        }
    };

    let client = Client::default();
    let mut current_snapshot = Arc::new(PositionSnapshot {
        by_asset: Arc::new(HashMap::new()),
    });

    while let Some(first_trigger) = refresh_rx.recv().await {
        let triggers = drain_triggers(first_trigger, &mut refresh_rx);
        let trigger_summary = summarize_triggers(&triggers);

        match sync_positions(&client, address, current_snapshot.as_ref()).await {
            Ok((next_snapshot, changed_assets)) => {
                let position_count = next_snapshot.by_asset.len();
                if changed_assets.is_empty() {
                    info!(
                        trigger = %trigger_summary,
                        position_count,
                        "positions 同步完成，仓位无变化"
                    );
                    current_snapshot = next_snapshot;
                    continue;
                }

                let changed_count = changed_assets.len();
                let changed_assets: Arc<[String]> = changed_assets.into();
                current_snapshot = next_snapshot.clone();

                info!(
                    trigger = %trigger_summary,
                    position_count,
                    changed_count,
                    changed_assets = ?changed_assets,
                    "positions 同步完成，检测到仓位变化"
                );

                if strategy_tx
                    .send(StrategyEvent::Positions(PositionsUpdateEvent {
                        snapshot: next_snapshot,
                        changed_assets,
                    }))
                    .await
                    .is_err()
                {
                    warn!("positions 广播仓位事件失败，策略通道已关闭");
                    return;
                }
            }
            Err(error) => {
                warn!(
                    trigger = %trigger_summary,
                    error = %error,
                    "positions 同步失败"
                );
            }
        }
    }
}

pub async fn run_simulated(
    mut fill_rx: tokio::sync::mpsc::Receiver<SimulatedFillEvent>,
    strategy_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
) {
    let mut current_snapshot = Arc::new(PositionSnapshot {
        by_asset: Arc::new(HashMap::new()),
    });

    while let Some(fill) = fill_rx.recv().await {
        let next_snapshot = apply_simulated_fill(current_snapshot.as_ref(), &fill);
        let changed_assets = diff_assets(
            current_snapshot.by_asset.as_ref(),
            next_snapshot.by_asset.as_ref(),
        );
        let position_count = next_snapshot.by_asset.len();

        if changed_assets.is_empty() {
            info!(
                target = "order",
                order_id = %fill.local_order_id,
                token = %fill.token,
                side = ?fill.side,
                price = %fill.price,
                size = %fill.size,
                position_count,
                "simulation 成交已处理，但仓位无变化"
            );
            current_snapshot = next_snapshot;
            continue;
        }

        let changed_count = changed_assets.len();
        let changed_assets: Arc<[String]> = changed_assets.into();
        current_snapshot = next_snapshot.clone();

        info!(
            target = "order",
            order_id = %fill.local_order_id,
            token = %fill.token,
            side = ?fill.side,
            price = %fill.price,
            size = %fill.size,
            position_count,
            changed_count,
            changed_assets = ?changed_assets,
            "simulation 成交已写入内存仓位，并广播仓位变化"
        );

        if strategy_tx
            .send(StrategyEvent::Positions(PositionsUpdateEvent {
                snapshot: next_snapshot,
                changed_assets,
            }))
            .await
            .is_err()
        {
            warn!("simulation 广播仓位事件失败，策略通道已关闭");
            return;
        }
    }
}

async fn sync_positions(
    client: &Client,
    address: Address,
    current_snapshot: &PositionSnapshot,
) -> anyhow::Result<(Arc<PositionSnapshot>, Vec<String>)> {
    let positions = fetch_all_positions(client, address).await?;
    let next_snapshot = Arc::new(PositionSnapshot {
        by_asset: Arc::new(
            positions
                .into_iter()
                .map(|position| (position.asset_id.clone(), position))
                .collect(),
        ),
    });
    let changed_assets = diff_assets(
        current_snapshot.by_asset.as_ref(),
        next_snapshot.by_asset.as_ref(),
    );
    Ok((next_snapshot, changed_assets))
}

async fn fetch_all_positions(
    client: &Client,
    address: Address,
) -> anyhow::Result<Vec<PositionView>> {
    let mut offset = 0;
    let mut positions = Vec::new();

    loop {
        let request = PositionsRequest::builder()
            .user(address)
            .size_threshold(Decimal::ZERO)
            .limit(PAGE_LIMIT)?
            .offset(offset)?
            .build();
        let batch = client.positions(&request).await?;
        let batch_len = batch.len();
        positions.extend(batch.into_iter().map(normalize_position));
        if batch_len < PAGE_LIMIT as usize {
            break;
        }
        offset += PAGE_LIMIT;
    }

    Ok(positions)
}

fn normalize_position(
    position: polymarket_client_sdk_v2::data::types::response::Position,
) -> PositionView {
    PositionView {
        asset_id: position.asset.to_string(),
        size: position.size,
        avg_price: position.avg_price,
        cur_price: position.cur_price,
        current_value: position.current_value,
        cash_pnl: position.cash_pnl,
        title: Arc::from(position.title),
        outcome: Arc::from(position.outcome),
    }
}

fn apply_simulated_fill(
    current_snapshot: &PositionSnapshot,
    fill: &SimulatedFillEvent,
) -> Arc<PositionSnapshot> {
    let mut next_positions = current_snapshot.by_asset.as_ref().clone();
    let mut position = next_positions
        .remove(&fill.token)
        .unwrap_or_else(|| PositionView {
            asset_id: fill.token.clone(),
            size: Decimal::ZERO,
            avg_price: fill.price,
            cur_price: fill.price,
            current_value: Decimal::ZERO,
            cash_pnl: Decimal::ZERO,
            title: Arc::from("simulation"),
            outcome: Arc::from("simulation"),
        });

    let delta = match fill.side {
        QuoteSide::Buy => fill.size,
        QuoteSide::Sell => -fill.size,
    };
    position.size += delta;
    position.avg_price = fill.price;
    position.cur_price = fill.price;
    position.current_value = position.size * fill.price;

    if position.size != Decimal::ZERO {
        next_positions.insert(fill.token.clone(), position);
    }

    Arc::new(PositionSnapshot {
        by_asset: Arc::new(next_positions),
    })
}

fn diff_assets(
    current: &HashMap<String, PositionView>,
    next: &HashMap<String, PositionView>,
) -> Vec<String> {
    let mut changed_assets = Vec::new();

    for (asset_id, next_position) in next {
        if current.get(asset_id) != Some(next_position) {
            changed_assets.push(asset_id.clone());
        }
    }

    for asset_id in current.keys() {
        if !next.contains_key(asset_id) {
            changed_assets.push(asset_id.clone());
        }
    }

    changed_assets.sort();
    changed_assets.dedup();
    changed_assets
}

fn drain_triggers(
    first_trigger: PositionRefreshTrigger,
    refresh_rx: &mut tokio::sync::mpsc::Receiver<PositionRefreshTrigger>,
) -> Vec<PositionRefreshTrigger> {
    let mut triggers = vec![first_trigger];
    while let Ok(trigger) = refresh_rx.try_recv() {
        triggers.push(trigger);
    }
    triggers
}

fn summarize_triggers(triggers: &[PositionRefreshTrigger]) -> &'static str {
    if triggers
        .iter()
        .any(|trigger| *trigger == PositionRefreshTrigger::Startup)
    {
        "startup"
    } else if triggers
        .iter()
        .any(|trigger| *trigger == PositionRefreshTrigger::OrderUpdate)
    {
        "order_update"
    } else {
        "order_placement"
    }
}
