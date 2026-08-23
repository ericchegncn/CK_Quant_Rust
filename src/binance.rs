use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    domain::{OrderIntent, OrderStatus, OrderUpdate, Position},
    exchange::Exchange,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use tokio::sync::{RwLock, broadcast};

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
