use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn order_side(self, reduce_only: bool) -> &'static str {
        match (self, reduce_only) {
            (Self::Long, false) | (Self::Short, true) => "BUY",
            (Self::Short, false) | (Self::Long, true) => "SELL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub pair: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub pair: String,
    pub side: Side,
    pub quantity: f64,
    pub entry_price: f64,
    pub leverage: f64,
    pub opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub client_id: Uuid,
    pub pair: String,
    pub side: Side,
    pub quantity: f64,
    pub price: Option<f64>,
    pub reduce_only: bool,
    pub leverage: f64,
    pub tag: String,
    pub created_at: DateTime<Utc>,
}

impl OrderIntent {
    pub fn market(
        pair: impl Into<String>,
        side: Side,
        quantity: f64,
        tag: impl Into<String>,
    ) -> Self {
        Self {
            client_id: Uuid::new_v4(),
            pair: pair.into(),
            side,
            quantity,
            price: None,
            reduce_only: false,
            leverage: 1.0,
            tag: tag.into(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub exchange_id: String,
    pub client_id: Uuid,
    pub pair: String,
    pub status: OrderStatus,
    pub filled_quantity: f64,
    pub average_price: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    RiskExit(OrderIntent),
    StrategyOrder(OrderIntent),
    OrderUpdate(OrderUpdate),
    Reconcile,
    Shutdown,
}
