use std::str::FromStr;

use anyhow::{Context, Result};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

use crate::domain::{OrderStatus, OrderUpdate};

#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
}

impl Storage {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        let storage = Self { pool };
        storage.migrate().await?;
        Ok(storage)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                client_id TEXT PRIMARY KEY NOT NULL,
                exchange_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                average_price REAL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_orders_active
                ON orders(status, pair, updated_at);
            CREATE TABLE IF NOT EXISTS engine_checkpoints (
                stream TEXT PRIMARY KEY NOT NULL,
                cursor TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_order(&self, order: &OrderUpdate) -> Result<()> {
        sqlx::query(r#"
            INSERT INTO orders(client_id, exchange_id, pair, status, filled_quantity, average_price, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_id) DO UPDATE SET
                exchange_id=excluded.exchange_id,
                status=excluded.status,
                filled_quantity=excluded.filled_quantity,
                average_price=excluded.average_price,
                updated_at=excluded.updated_at
        "#)
            .bind(order.client_id.to_string())
            .bind(&order.exchange_id)
            .bind(&order.pair)
            .bind(status_name(order.status))
            .bind(order.filled_quantity)
            .bind(order.average_price)
            .bind(order.updated_at.to_rfc3339())
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn active_order_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM orders WHERE status IN ('pending','open','partially_filled')")
            .fetch_one(&self.pool).await.context("count active orders")?;
        Ok(row.try_get("count")?)
    }

    pub async fn total_order_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM orders")
            .fetch_one(&self.pool)
            .await
            .context("count all orders")?;
        Ok(row.try_get("count")?)
    }
}

fn status_name(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Pending => "pending",
        OrderStatus::Open => "open",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
    }
}

#[allow(dead_code)]
fn parse_uuid(value: &str) -> Result<Uuid> {
    Ok(Uuid::parse_str(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn indexes_only_active_order_query() {
        let storage = Storage::connect("sqlite::memory:").await.unwrap();
        storage
            .upsert_order(&OrderUpdate {
                exchange_id: "1".into(),
                client_id: Uuid::new_v4(),
                pair: "BTC/USDT:USDT".into(),
                status: OrderStatus::Open,
                filled_quantity: 0.0,
                average_price: None,
                updated_at: Utc::now(),
            })
            .await
            .unwrap();
        assert_eq!(storage.active_order_count().await.unwrap(), 1);
    }
}
