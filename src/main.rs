use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use axum::{Json, Router, routing::get};
use ck_quant_rust::{
    backtest, config::Config, engine::Engine, exchange::PaperExchange, storage::Storage,
    strategy::SampleEmaStrategy,
};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use tracing::info;

#[derive(Parser)]
#[command(name = "ck-quant-rust", version, about = "Low-latency CK Quant engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Validate {
        #[arg(long)]
        config: PathBuf,
    },
    Serve {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
    },
    Backtest {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        candles: PathBuf,
        #[arg(long)]
        pair: String,
        #[arg(long, default_value_t = 10_000.0)]
        initial_equity: f64,
    },
    Benchmark {
        #[arg(long, default_value_t = 1000)]
        orders: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ck_quant_rust=info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Validate { config } => {
            let config = Config::load(config)?;
            println!(
                "configuration valid: exchange={}, dry_run={}, pairs={}",
                config.exchange.name,
                config.dry_run,
                config.exchange.pair_whitelist.len()
            );
        }
        Command::Serve { config, listen } => serve(Config::load(config)?, listen).await?,
        Command::Backtest {
            config,
            candles,
            pair,
            initial_equity,
        } => {
            let config = Config::load(config)?;
            let candles = backtest::load_candles_csv(candles, &pair)?;
            let report = backtest::run(
                &SampleEmaStrategy,
                &candles,
                config.stake_amount,
                initial_equity,
            );
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Benchmark { orders } => benchmark(orders).await?,
    }
    Ok(())
}

async fn benchmark(orders: usize) -> Result<()> {
    use ck_quant_rust::domain::{EngineEvent, OrderIntent, Side};
    use std::time::{Duration, Instant};

    let storage = Storage::connect("sqlite::memory:").await?;
    let (engine, handle) = Engine::new(Arc::new(PaperExchange::default()), storage.clone());
    let task = engine.spawn();
    let started = Instant::now();
    for index in 0..orders {
        let intent = OrderIntent::market(
            "BTC/USDT:USDT",
            Side::Long,
            0.001,
            format!("benchmark-{index}"),
        );
        handle.strategy(EngineEvent::StrategyOrder(intent)).await?;
    }
    loop {
        if storage.total_order_count().await? >= orders as i64 {
            break;
        }
        if started.elapsed() > Duration::from_secs(30) {
            anyhow::bail!("benchmark timed out")
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let elapsed = started.elapsed();
    handle.shutdown().await?;
    task.await??;
    println!(
        "orders={orders} elapsed_ms={} throughput_orders_per_sec={:.0}",
        elapsed.as_millis(),
        orders as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

async fn serve(config: Config, listen: SocketAddr) -> Result<()> {
    let storage = Storage::connect(&config.database_url).await?;
    let (engine, handle) = Engine::new(Arc::new(PaperExchange::default()), storage.clone());
    let task = engine.spawn();
    let app = Router::new()
        .route(
            "/api/v1/ping",
            get(|| async { Json(json!({"status": "pong"})) }),
        )
        .route("/api/v1/health", get(move || health(storage.clone())));
    info!(%listen, dry_run=config.dry_run, "CK Quant Rust server started");
    axum::serve(tokio::net::TcpListener::bind(listen).await?, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    handle.shutdown().await?;
    task.await??;
    Ok(())
}

async fn health(storage: Storage) -> Json<Value> {
    match storage.active_order_count().await {
        Ok(active_orders) => Json(json!({"status": "ok", "active_orders": active_orders})),
        Err(error) => Json(json!({"status": "degraded", "error": error.to_string()})),
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
