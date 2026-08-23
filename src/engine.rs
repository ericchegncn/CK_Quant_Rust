use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use tokio::{
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::{error, info, warn};

use crate::{domain::EngineEvent, exchange::Exchange, storage::Storage};

#[derive(Clone)]
pub struct EngineHandle {
    risk_tx: mpsc::Sender<EngineEvent>,
    strategy_tx: mpsc::Sender<EngineEvent>,
    background_tx: mpsc::Sender<EngineEvent>,
}

impl EngineHandle {
    pub async fn risk_exit(&self, event: EngineEvent) -> Result<()> {
        self.risk_tx.send(event).await.map_err(Into::into)
    }
    pub async fn strategy(&self, event: EngineEvent) -> Result<()> {
        self.strategy_tx.send(event).await.map_err(Into::into)
    }
    pub fn reconcile(&self) -> Result<()> {
        self.background_tx
            .try_send(EngineEvent::Reconcile)
            .map_err(|error| anyhow::anyhow!("reconcile queue unavailable: {error}"))
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.risk_tx
            .send(EngineEvent::Shutdown)
            .await
            .map_err(Into::into)
    }
}

pub struct Engine<E: Exchange> {
    exchange: Arc<E>,
    storage: Storage,
    risk_rx: mpsc::Receiver<EngineEvent>,
    strategy_rx: mpsc::Receiver<EngineEvent>,
    background_rx: mpsc::Receiver<EngineEvent>,
    max_order_latency: Duration,
}

impl<E: Exchange> Engine<E> {
    pub fn new(exchange: Arc<E>, storage: Storage) -> (Self, EngineHandle) {
        let (risk_tx, risk_rx) = mpsc::channel(2048);
        let (strategy_tx, strategy_rx) = mpsc::channel(8192);
        let (background_tx, background_rx) = mpsc::channel(128);
        let handle = EngineHandle {
            risk_tx,
            strategy_tx,
            background_tx,
        };
        (
            Self {
                exchange,
                storage,
                risk_rx,
                strategy_rx,
                background_rx,
                max_order_latency: Duration::from_secs(2),
            },
            handle,
        )
    }

    pub fn spawn(self) -> JoinHandle<Result<()>> {
        tokio::spawn(self.run())
    }

    async fn run(self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();

        let Self {
            exchange,
            storage,
            mut risk_rx,
            mut strategy_rx,
            mut background_rx,
            max_order_latency,
        } = self;

        let risk_exchange = exchange.clone();
        let risk_storage = storage.clone();
        let mut risk_shutdown = shutdown_rx.clone();
        let risk_shutdown_tx = shutdown_tx.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    Some(event) = risk_rx.recv() => {
                        if matches!(event, EngineEvent::Shutdown) {
                            let _ = risk_shutdown_tx.send(true);
                            break;
                        }
                        process_event(&*risk_exchange, &risk_storage, event, "risk", max_order_latency).await?;
                    }
                    _ = risk_shutdown.changed() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let strategy_exchange = exchange.clone();
        let strategy_storage = storage.clone();
        let mut strategy_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    Some(event) = strategy_rx.recv() => process_event(&*strategy_exchange, &strategy_storage, event, "strategy", max_order_latency).await?,
                    _ = strategy_shutdown.changed() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let background_exchange = exchange.clone();
        let background_storage = storage.clone();
        let mut background_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    Some(event) = background_rx.recv() => process_event(&*background_exchange, &background_storage, event, "background", max_order_latency).await?,
                    _ = background_shutdown.changed() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let mut stream = exchange.subscribe();
        let mut stream_shutdown = shutdown_rx;
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    Ok(update) = stream.recv() => storage.upsert_order(&update).await?,
                    _ = stream_shutdown.changed() => break,
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        while let Some(result) = tasks.join_next().await {
            result??;
            if *shutdown_tx.borrow() {
                continue;
            }
            let _ = shutdown_tx.send(true);
        }
        Ok(())
    }
}

async fn process_event<E: Exchange>(
    exchange: &E,
    storage: &Storage,
    event: EngineEvent,
    lane: &'static str,
    max_order_latency: Duration,
) -> Result<()> {
    match event {
        EngineEvent::RiskExit(intent) | EngineEvent::StrategyOrder(intent) => {
            let queued = intent.created_at;
            let started = Instant::now();
            let update = exchange.place_order(&intent).await?;
            storage.upsert_order(&update).await?;
            let elapsed = started.elapsed();
            let queue_ms = (chrono::Utc::now() - queued).num_milliseconds().max(0);
            info!(lane, pair=%intent.pair, client_id=%intent.client_id, queue_ms, latency_ms=elapsed.as_millis(), "order submitted");
            if elapsed > max_order_latency {
                warn!(lane, pair=%intent.pair, latency_ms=elapsed.as_millis(), "order latency SLO exceeded");
            }
        }
        EngineEvent::OrderUpdate(update) => storage.upsert_order(&update).await?,
        EngineEvent::Reconcile => {
            // Reconcile only active orders; never scan historical orders in the critical loop.
            for update in exchange.active_orders().await? {
                if let Err(error) = storage.upsert_order(&update).await {
                    error!(%error, exchange_id=%update.exchange_id, "reconcile write failed");
                }
            }
        }
        EngineEvent::Shutdown => {}
    }
    Ok(())
}

pub fn validate_risk_event(event: &EngineEvent) -> Result<()> {
    if matches!(event, EngineEvent::RiskExit(_)) {
        Ok(())
    } else {
        bail!("risk lane accepts exits only")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{OrderIntent, OrderStatus, OrderUpdate, Position, Side},
        exchange::{Exchange, PaperExchange},
    };
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn processes_risk_exit_and_persists_it() {
        let storage = Storage::connect("sqlite::memory:").await.unwrap();
        let (engine, handle) = Engine::new(Arc::new(PaperExchange::default()), storage.clone());
        let task = engine.spawn();
        let mut intent = OrderIntent::market("BTC/USDT:USDT", Side::Long, 0.01, "紧急平仓");
        intent.reduce_only = true;
        handle
            .risk_exit(EngineEvent::RiskExit(intent))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        handle.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(storage.active_order_count().await.unwrap(), 0);
    }

    struct SlowStrategyExchange {
        updates: broadcast::Sender<OrderUpdate>,
    }

    impl SlowStrategyExchange {
        fn new() -> Self {
            let (updates, _) = broadcast::channel(32);
            Self { updates }
        }
    }

    #[async_trait]
    impl Exchange for SlowStrategyExchange {
        async fn place_order(&self, intent: &OrderIntent) -> Result<OrderUpdate> {
            if intent.tag == "slow" {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            let update = OrderUpdate {
                exchange_id: intent.tag.clone(),
                client_id: intent.client_id,
                pair: intent.pair.clone(),
                status: OrderStatus::Filled,
                filled_quantity: intent.quantity,
                average_price: None,
                updated_at: Utc::now(),
            };
            let _ = self.updates.send(update.clone());
            Ok(update)
        }
        async fn cancel_order(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        async fn active_orders(&self) -> Result<Vec<OrderUpdate>> {
            Ok(vec![])
        }
        async fn positions(&self) -> Result<Vec<Position>> {
            Ok(vec![])
        }
        fn subscribe(&self) -> broadcast::Receiver<OrderUpdate> {
            self.updates.subscribe()
        }
    }

    #[tokio::test]
    async fn risk_lane_does_not_wait_for_slow_strategy_request() {
        let exchange = Arc::new(SlowStrategyExchange::new());
        let mut updates = exchange.subscribe();
        let storage = Storage::connect("sqlite::memory:").await.unwrap();
        let (engine, handle) = Engine::new(exchange, storage);
        let task = engine.spawn();
        handle
            .strategy(EngineEvent::StrategyOrder(OrderIntent::market(
                "BTC/USDT:USDT",
                Side::Long,
                1.0,
                "slow",
            )))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut risk = OrderIntent::market("BTC/USDT:USDT", Side::Long, 1.0, "risk");
        risk.reduce_only = true;
        let started = Instant::now();
        handle.risk_exit(EngineEvent::RiskExit(risk)).await.unwrap();
        let first = tokio::time::timeout(Duration::from_millis(100), updates.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.exchange_id, "risk");
        assert!(started.elapsed() < Duration::from_millis(100));
        handle.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
    }
}
