use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

use crate::domain::{OrderIntent, OrderStatus, OrderUpdate, Position};

#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    async fn place_order(&self, intent: &OrderIntent) -> Result<OrderUpdate>;
    async fn cancel_order(&self, pair: &str, exchange_id: &str) -> Result<()>;
    async fn active_orders(&self) -> Result<Vec<OrderUpdate>>;
    async fn positions(&self) -> Result<Vec<Position>>;
    fn subscribe(&self) -> broadcast::Receiver<OrderUpdate>;
}

#[derive(Clone)]
pub struct PaperExchange {
    orders: Arc<Mutex<HashMap<Uuid, OrderUpdate>>>,
    updates: broadcast::Sender<OrderUpdate>,
}

impl Default for PaperExchange {
    fn default() -> Self {
        let (updates, _) = broadcast::channel(1024);
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            updates,
        }
    }
}

#[async_trait]
impl Exchange for PaperExchange {
    async fn place_order(&self, intent: &OrderIntent) -> Result<OrderUpdate> {
        let update = OrderUpdate {
            exchange_id: format!("paper-{}", intent.client_id),
            client_id: intent.client_id,
            pair: intent.pair.clone(),
            status: OrderStatus::Filled,
            filled_quantity: intent.quantity,
            average_price: intent.price,
            updated_at: Utc::now(),
        };
        self.orders
            .lock()
            .await
            .insert(intent.client_id, update.clone());
        let _ = self.updates.send(update.clone());
        Ok(update)
    }

    async fn cancel_order(&self, _pair: &str, exchange_id: &str) -> Result<()> {
        let mut orders = self.orders.lock().await;
        if let Some(order) = orders.values_mut().find(|o| o.exchange_id == exchange_id) {
            order.status = OrderStatus::Cancelled;
            order.updated_at = Utc::now();
            let _ = self.updates.send(order.clone());
        }
        Ok(())
    }

    async fn active_orders(&self) -> Result<Vec<OrderUpdate>> {
        Ok(self
            .orders
            .lock()
            .await
            .values()
            .filter(|order| {
                matches!(
                    order.status,
                    OrderStatus::Open | OrderStatus::PartiallyFilled | OrderStatus::Pending
                )
            })
            .cloned()
            .collect())
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        Ok(vec![])
    }
    fn subscribe(&self) -> broadcast::Receiver<OrderUpdate> {
        self.updates.subscribe()
    }
}
