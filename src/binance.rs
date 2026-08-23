use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    domain::{OrderIntent, OrderStatus, OrderUpdate, Position},
    exchange::Exchange,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceUsdm {
    client: Client,
    base_url: String,
    api_key: String,
    secret: String,
    active: Arc<RwLock<HashMap<String, OrderUpdate>>>,
    updates: broadcast::Sender<OrderUpdate>,
}

impl BinanceUsdm {
    pub fn new(api_key: impl Into<String>, secret: impl Into<String>) -> Result<Self> {
        let (updates, _) = broadcast::channel(4096);
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(3))
                .pool_max_idle_per_host(32)
                .build()?,
            base_url: "https://fapi.binance.com".into(),
            api_key: api_key.into(),
            secret: secret.into(),
            active: Arc::new(RwLock::new(HashMap::new())),
            updates,
        })
    }

    fn symbol(pair: &str) -> String {
        pair.split(':').next().unwrap_or(pair).replace('/', "")
    }

    fn signed_query(&self, mut params: Vec<(&str, String)>) -> Result<String> {
        params.push(("timestamp", Utc::now().timestamp_millis().to_string()));
        params.push(("recvWindow", "3000".into()));
        let query = serde_urlencoded::to_string(params)?;
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())?;
        mac.update(query.as_bytes());
        let signature = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok(format!("{query}&signature={signature}"))
    }

    async fn request_order(&self, intent: &OrderIntent) -> Result<BinanceOrder> {
        let mut params = vec![
            ("symbol", Self::symbol(&intent.pair)),
            ("side", intent.side.order_side(intent.reduce_only).into()),
            (
                "type",
                if intent.price.is_some() {
                    "LIMIT".into()
                } else {
                    "MARKET".into()
                },
            ),
            ("quantity", intent.quantity.to_string()),
            (
                "newClientOrderId",
                format!("ckq_{}", intent.client_id.simple()),
            ),
            ("reduceOnly", intent.reduce_only.to_string()),
            ("newOrderRespType", "RESULT".into()),
        ];
        if let Some(price) = intent.price {
            params.push(("price", price.to_string()));
            params.push(("timeInForce", "GTC".into()));
        }
        let query = self.signed_query(params)?;
        let response = self
            .client
            .post(format!("{}/fapi/v1/order?{}", self.base_url, query))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Binance order request")?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            bail!("Binance order rejected ({status}): {body}")
        }
        Ok(serde_json::from_str(&body)?)
    }

    async fn new_listen_key(&self) -> Result<String> {
        let response = self
            .client
            .post(format!("{}/fapi/v1/listenKey", self.base_url))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            bail!("Binance listen-key request rejected ({status}): {body}")
        }
        Ok(serde_json::from_str::<ListenKey>(&body)?.listen_key)
    }

    async fn keepalive_listen_key(&self) -> Result<()> {
        let response = self
            .client
            .put(format!("{}/fapi/v1/listenKey", self.base_url))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "Binance listen-key keepalive rejected: {}",
                response.text().await?
            )
        }
        Ok(())
    }

    /// Starts the USD-M user-data stream with reconnect and listen-key renewal.
    /// The 2026 routed endpoint is required for private events.
    pub fn spawn_user_data_stream(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match self.run_user_data_session().await {
                    Ok(()) => backoff = Duration::from_secs(1),
                    Err(error) => warn!(
                        %error,
                        retry_seconds = backoff.as_secs(),
                        "Binance user-data stream disconnected"
                    ),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        })
    }

    async fn run_user_data_session(&self) -> Result<()> {
        let listen_key = self.new_listen_key().await?;
        let url = format!("wss://fstream.binance.com/private/ws/{listen_key}");
        let (mut stream, _) = connect_async(&url)
            .await
            .context("connect Binance user-data WebSocket")?;
        let mut keepalive = tokio::time::interval(Duration::from_secs(30 * 60));
        keepalive.tick().await;
        loop {
            tokio::select! {
                _ = keepalive.tick() => self.keepalive_listen_key().await?,
                message = stream.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(update) = parse_user_order_event(&text)? {
                            if matches!(update.status, OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected) {
                                self.active.write().await.remove(&update.exchange_id);
                            } else {
                                self.active.write().await.insert(update.exchange_id.clone(), update.clone());
                            }
                            let _ = self.updates.send(update);
                        }
                        if text.contains("listenKeyExpired") { bail!("Binance listen key expired") }
                    }
                    Some(Ok(Message::Ping(payload))) => stream.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(frame))) => bail!("Binance closed user stream: {frame:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => bail!("Binance user stream ended"),
                }
            }
        }
    }
}

#[async_trait]
impl Exchange for BinanceUsdm {
    async fn place_order(&self, intent: &OrderIntent) -> Result<OrderUpdate> {
        let result = self.request_order(intent).await?;
        let update = OrderUpdate {
            exchange_id: result.order_id.to_string(),
            client_id: intent.client_id,
            pair: intent.pair.clone(),
            status: parse_status(&result.status),
            filled_quantity: result.executed_qty.parse().unwrap_or_default(),
            average_price: result.avg_price.and_then(|v| v.parse().ok()),
            updated_at: Utc::now(),
        };
        if matches!(
            update.status,
            OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
        ) {
            self.active
                .write()
                .await
                .insert(update.exchange_id.clone(), update.clone());
        }
        let _ = self.updates.send(update.clone());
        Ok(update)
    }

    async fn cancel_order(&self, pair: &str, exchange_id: &str) -> Result<()> {
        let query = self.signed_query(vec![
            ("symbol", Self::symbol(pair)),
            ("orderId", exchange_id.into()),
        ])?;
        let response = self
            .client
            .delete(format!("{}/fapi/v1/order?{}", self.base_url, query))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("Binance cancel rejected: {}", response.text().await?)
        }
        self.active.write().await.remove(exchange_id);
        Ok(())
    }

    async fn active_orders(&self) -> Result<Vec<OrderUpdate>> {
        // The user-data WebSocket owns normal state transitions. This bounded
        // cache is used for periodic reconciliation instead of historical scans.
        Ok(self.active.read().await.values().cloned().collect())
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        Ok(vec![])
    }
    fn subscribe(&self) -> broadcast::Receiver<OrderUpdate> {
        self.updates.subscribe()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceOrder {
    order_id: i64,
    status: String,
    executed_qty: String,
    avg_price: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListenKey {
    listen_key: String,
}

#[derive(Deserialize)]
struct UserEvent {
    #[serde(rename = "e")]
    event: String,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "o")]
    order: Option<UserOrder>,
}

#[derive(Deserialize)]
struct UserOrder {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_id: String,
    #[serde(rename = "i")]
    exchange_id: i64,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "z")]
    filled_quantity: String,
    #[serde(rename = "ap")]
    average_price: String,
}

fn parse_user_order_event(text: &str) -> Result<Option<OrderUpdate>> {
    let event: UserEvent = serde_json::from_str(text)?;
    if event.event != "ORDER_TRADE_UPDATE" {
        return Ok(None);
    }
    let Some(order) = event.order else {
        return Ok(None);
    };
    let Some(raw_uuid) = order.client_id.strip_prefix("ckq_") else {
        return Ok(None);
    };
    let client_id = Uuid::parse_str(raw_uuid).context("invalid CK Quant client order ID")?;
    let pair = if let Some(base) = order.symbol.strip_suffix("USDT") {
        format!("{base}/USDT:USDT")
    } else {
        order.symbol
    };
    Ok(Some(OrderUpdate {
        exchange_id: order.exchange_id.to_string(),
        client_id,
        pair,
        status: parse_status(&order.status),
        filled_quantity: order.filled_quantity.parse().unwrap_or_default(),
        average_price: order
            .average_price
            .parse()
            .ok()
            .filter(|price| *price > 0.0),
        updated_at: event
            .event_time
            .and_then(chrono::DateTime::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    }))
}

fn parse_status(value: &str) -> OrderStatus {
    match value {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "EXPIRED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_owned_order_trade_update() {
        let event = r#"{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"o":{"s":"BTCUSDT","c":"ckq_85f8473ad7ce431284d37b5305305f17","i":8886774,"X":"PARTIALLY_FILLED","z":"0.002","ap":"27123.50"}}"#;
        let update = parse_user_order_event(event).unwrap().unwrap();
        assert_eq!(update.pair, "BTC/USDT:USDT");
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.filled_quantity, 0.002);
        assert_eq!(update.average_price, Some(27123.5));
    }

    #[test]
    fn ignores_orders_not_owned_by_ck_quant() {
        let event = r#"{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"o":{"s":"BTCUSDT","c":"manual-order","i":1,"X":"NEW","z":"0","ap":"0"}}"#;
        assert!(parse_user_order_event(event).unwrap().is_none());
    }
}
