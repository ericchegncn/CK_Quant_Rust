use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::domain::{OrderStatus, OrderUpdate};

/// Persistence is split so lifetime history can never enter the trading hot path.
#[derive(Clone)]
pub struct Storage {
    active_pool: SqlitePool,
    history_pool: SqlitePool,
}

impl Storage {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let history_url = history_database_url(database_url)?;
        Self::connect_split(database_url, &history_url).await
    }

    pub async fn connect_split(
        active_database_url: &str,
        history_database_url: &str,
    ) -> Result<Self> {
        let active_pool = connect_pool(active_database_url, 2).await?;
        let history_pool = connect_pool(history_database_url, 2).await?;
        let storage = Self {
            active_pool,
            history_pool,
        };
        storage.migrate().await?;
        Ok(storage)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS active_orders (
                client_id TEXT PRIMARY KEY NOT NULL,
                exchange_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                average_price REAL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_active_orders_pair_updated
                ON active_orders(pair, updated_at);
            CREATE TABLE IF NOT EXISTS engine_checkpoints (
                stream TEXT PRIMARY KEY NOT NULL,
                cursor TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.active_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS order_history (
                client_id TEXT PRIMARY KEY NOT NULL,
                exchange_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                average_price REAL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_order_history_updated
                ON order_history(updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_order_history_pair_updated
                ON order_history(pair, updated_at DESC);
            CREATE TABLE IF NOT EXISTS account_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                closed_trade_count INTEGER NOT NULL DEFAULT 0,
                winning_trade_count INTEGER NOT NULL DEFAULT 0,
                losing_trade_count INTEGER NOT NULL DEFAULT 0,
                closed_profit REAL NOT NULL DEFAULT 0,
                gross_profit REAL NOT NULL DEFAULT 0,
                gross_loss REAL NOT NULL DEFAULT 0,
                peak_equity REAL NOT NULL DEFAULT 0,
                max_drawdown REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
        "#,
        )
        .execute(&self.history_pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_order(&self, order: &OrderUpdate) -> Result<()> {
        if is_active(order.status) {
            upsert(&self.active_pool, "active_orders", order).await?;
            return Ok(());
        }

        // Archive first so a crash cannot lose a terminal update. Reconciliation heals a
        // possible stale active row after a crash between the two operations.
        upsert(&self.history_pool, "order_history", order).await?;
        sqlx::query("DELETE FROM active_orders WHERE client_id = ?")
            .bind(order.client_id.to_string())
            .execute(&self.active_pool)
            .await?;
        Ok(())
    }

    pub async fn active_order_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM active_orders")
            .fetch_one(&self.active_pool)
            .await
            .context("count active orders")?;
        Ok(row.try_get("count")?)
    }

    pub async fn total_order_count(&self) -> Result<i64> {
        let active = self.active_order_count().await?;
        let row = sqlx::query("SELECT COUNT(*) AS count FROM order_history")
            .fetch_one(&self.history_pool)
            .await
            .context("count historical orders")?;
        Ok(active + row.try_get::<i64, _>("count")?)
    }
}

async fn connect_pool(database_url: &str, max_connections: u32) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    Ok(SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await?)
}

async fn upsert(pool: &SqlitePool, table: &str, order: &OrderUpdate) -> Result<()> {
    let sql = format!(
        r#"
        INSERT INTO {table}(client_id, exchange_id, pair, status, filled_quantity, average_price, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_id) DO UPDATE SET
            exchange_id=excluded.exchange_id,
            pair=excluded.pair,
            status=excluded.status,
            filled_quantity=excluded.filled_quantity,
            average_price=excluded.average_price,
            updated_at=excluded.updated_at
        "#
    );
    sqlx::query(&sql)
        .bind(order.client_id.to_string())
        .bind(&order.exchange_id)
        .bind(&order.pair)
        .bind(status_name(order.status))
        .bind(order.filled_quantity)
        .bind(order.average_price)
        .bind(order.updated_at.to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}

fn is_active(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
    )
}

fn history_database_url(database_url: &str) -> Result<String> {
    if database_url == "sqlite::memory:" {
        return Ok(format!(
            "sqlite:file:ck_quant_history_{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        ));
    }
    let (prefix, raw_path) = if let Some(path) = database_url.strip_prefix("sqlite://") {
        ("sqlite://", path)
    } else if let Some(path) = database_url.strip_prefix("sqlite:") {
        ("sqlite:", path)
    } else {
        bail!("only SQLite storage is currently supported: {database_url}");
    };
    let path = Path::new(raw_path);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .context("database URL must end in a file name")?;
    let history_name = match path.extension().and_then(|value| value.to_str()) {
        Some(extension) => format!("{stem}.history.{extension}"),
        None => format!("{stem}.history.sqlite"),
    };
    Ok(format!(
        "{prefix}{}",
        path.with_file_name(history_name).to_string_lossy()
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn update(client_id: Uuid, status: OrderStatus) -> OrderUpdate {
        OrderUpdate {
            exchange_id: client_id.to_string(),
            client_id,
            pair: "BTC/USDT:USDT".into(),
            status,
            filled_quantity: 1.0,
            average_price: Some(60_000.0),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn terminal_orders_leave_the_latency_critical_database() {
        let dir = tempfile::tempdir().unwrap();
        let active = format!("sqlite://{}", dir.path().join("active.sqlite").display());
        let history = format!("sqlite://{}", dir.path().join("history.sqlite").display());
        let storage = Storage::connect_split(&active, &history).await.unwrap();
        let client_id = Uuid::new_v4();

        storage
            .upsert_order(&update(client_id, OrderStatus::Open))
            .await
            .unwrap();
        assert_eq!(storage.active_order_count().await.unwrap(), 1);
        storage
            .upsert_order(&update(client_id, OrderStatus::Filled))
            .await
            .unwrap();

        assert_eq!(storage.active_order_count().await.unwrap(), 0);
        assert_eq!(storage.total_order_count().await.unwrap(), 1);
        assert!(dir.path().join("active.sqlite").exists());
        assert!(dir.path().join("history.sqlite").exists());
    }

    #[tokio::test]
    async fn historical_growth_does_not_grow_the_active_table() {
        let dir = tempfile::tempdir().unwrap();
        let active = format!("sqlite://{}", dir.path().join("active.sqlite").display());
        let history = format!("sqlite://{}", dir.path().join("history.sqlite").display());
        let storage = Storage::connect_split(&active, &history).await.unwrap();

        for _ in 0..2_000 {
            storage
                .upsert_order(&update(Uuid::new_v4(), OrderStatus::Filled))
                .await
                .unwrap();
        }

        assert_eq!(storage.total_order_count().await.unwrap(), 2_000);
        assert_eq!(storage.active_order_count().await.unwrap(), 0);
        storage
            .upsert_order(&update(Uuid::new_v4(), OrderStatus::Open))
            .await
            .unwrap();
        assert_eq!(storage.active_order_count().await.unwrap(), 1);
    }

    #[test]
    fn derives_a_separate_history_file() {
        assert_eq!(
            history_database_url("sqlite://user_data/trades.sqlite").unwrap(),
            "sqlite://user_data/trades.history.sqlite"
        );
    }
}
