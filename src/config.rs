use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_timeframe() -> String {
    "15m".into()
}
fn default_database() -> String {
    "sqlite://user_data/trades.sqlite".into()
}
fn default_throttle() -> u64 {
    1
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub secret: String,
    #[serde(default)]
    pub pair_whitelist: Vec<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl std::fmt::Debug for ExchangeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeConfig")
            .field("name", &self.name)
            .field("key", &if self.key.is_empty() { "" } else { "***" })
            .field("secret", &if self.secret.is_empty() { "" } else { "***" })
            .field("pair_whitelist", &self.pair_whitelist)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalsConfig {
    #[serde(default = "default_throttle")]
    pub process_throttle_secs: u64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Default for InternalsConfig {
    fn default() -> Self {
        Self {
            process_throttle_secs: default_throttle(),
            extra: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_timeframe")]
    pub timeframe: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub trading_mode: String,
    #[serde(default)]
    pub margin_mode: String,
    #[serde(default)]
    pub stake_currency: String,
    pub stake_amount: f64,
    pub max_open_trades: i64,
    pub exchange: ExchangeConfig,
    #[serde(default)]
    pub internals: InternalsConfig,
    #[serde(default = "default_database")]
    pub database_url: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let config: Self = json5::from_str(&raw)
            .with_context(|| format!("invalid Freqtrade-compatible config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.stake_amount <= 0.0 {
            bail!("stake_amount must be positive")
        }
        if self.max_open_trades == 0 || self.max_open_trades < -1 {
            bail!("max_open_trades must be -1 (unlimited) or positive")
        }
        if self.exchange.name.trim().is_empty() {
            bail!("exchange.name is required")
        }
        if !self.dry_run && (self.exchange.key.is_empty() || self.exchange.secret.is_empty()) {
            bail!("live mode requires exchange.key and exchange.secret")
        }
        Ok(())
    }

    pub fn throttle(&self) -> Duration {
        Duration::from_secs(self.internals.process_throttle_secs.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_comments_and_unknown_freqtrade_fields_without_leaking_keys() {
        let config: Config = json5::from_str(
            r#"{
          // Freqtrade-style comments are accepted.
          dry_run: true, stake_amount: 10, max_open_trades: 3,
          exchange: { name: 'binance', key: 'k', secret: 's' },
          unknown_future_field: 42
        }"#,
        )
        .unwrap();
        config.validate().unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("key: \"k\""));
        assert!(!debug.contains("secret: \"s\""));
    }
}
