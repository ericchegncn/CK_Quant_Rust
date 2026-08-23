use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::{
    domain::{Candle, Position},
    strategy::{Strategy, StrategyContext},
};

#[derive(Debug, Default, Serialize)]
pub struct BacktestReport {
    pub candles: usize,
    pub signals: usize,
    pub final_equity: f64,
    pub max_drawdown: f64,
}

pub fn load_candles_csv(path: impl AsRef<Path>, pair: &str) -> Result<Vec<Candle>> {
    let mut reader = csv::Reader::from_path(path)?;
    let mut candles = Vec::new();
    for row in reader.deserialize::<CsvCandle>() {
        let row = row?;
        candles.push(Candle {
            pair: pair.to_owned(),
            timestamp: row.date,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
        });
    }
    Ok(candles)
}

pub fn run<S: Strategy>(
    strategy: &S,
    candles: &[Candle],
    stake_amount: f64,
    initial_equity: f64,
) -> BacktestReport {
    let mut report = BacktestReport {
        final_equity: initial_equity,
        ..Default::default()
    };
    let position: Option<Position> = None;
    let mut peak = initial_equity;
    for end in 1..=candles.len() {
        let context = StrategyContext {
            candles: &candles[..end],
            position: position.as_ref(),
            max_leverage: 1.0,
            stake_amount,
        };
        report.signals += strategy.on_candle(&context).len();
        peak = peak.max(report.final_equity);
        report.max_drawdown = report
            .max_drawdown
            .max((peak - report.final_equity) / peak.max(f64::EPSILON));
    }
    report.candles = candles.len();
    report
}

#[derive(serde::Deserialize)]
struct CsvCandle {
    #[serde(alias = "timestamp")]
    date: chrono::DateTime<chrono::Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}
