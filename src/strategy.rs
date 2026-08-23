use crate::domain::{Candle, OrderIntent, Position, Side};

#[derive(Debug, Clone)]
pub struct StrategyContext<'a> {
    pub candles: &'a [Candle],
    pub position: Option<&'a Position>,
    pub max_leverage: f64,
    pub stake_amount: f64,
}

pub trait Strategy: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn timeframe(&self) -> &'static str;
    fn startup_candles(&self) -> usize {
        200
    }
    fn on_candle(&self, context: &StrategyContext<'_>) -> Vec<OrderIntent>;
    fn custom_exit(&self, _context: &StrategyContext<'_>, _current_rate: f64) -> Option<String> {
        None
    }
    fn leverage(&self, _context: &StrategyContext<'_>, _side: Side) -> f64 {
        1.0
    }
}

/// Public demonstration strategy. It exists to validate the engine and is not
/// CK_Trend or an approximation of the owner's private strategy.
#[derive(Debug, Default)]
pub struct SampleEmaStrategy;

impl Strategy for SampleEmaStrategy {
    fn name(&self) -> &'static str {
        "SampleEmaStrategy"
    }
    fn timeframe(&self) -> &'static str {
        "15m"
    }
    fn startup_candles(&self) -> usize {
        32
    }

    fn on_candle(&self, context: &StrategyContext<'_>) -> Vec<OrderIntent> {
        if context.position.is_some() || context.candles.len() < self.startup_candles() {
            return vec![];
        }
        let closes: Vec<f64> = context.candles.iter().map(|c| c.close).collect();
        let fast = ema(&closes, 12).unwrap_or_default();
        let slow = ema(&closes, 26).unwrap_or_default();
        let previous_fast = ema(&closes[..closes.len() - 1], 12).unwrap_or_default();
        let previous_slow = ema(&closes[..closes.len() - 1], 26).unwrap_or_default();
        let side = if previous_fast <= previous_slow && fast > slow {
            Some(Side::Long)
        } else if previous_fast >= previous_slow && fast < slow {
            Some(Side::Short)
        } else {
            None
        };
        side.map(|side| {
            let last = context.candles.last().expect("length checked");
            let quantity = context.stake_amount / last.close;
            OrderIntent::market(last.pair.clone(), side, quantity, "sample_ema_cross")
        })
        .into_iter()
        .collect()
    }
}

pub fn ema(values: &[f64], period: usize) -> Option<f64> {
    if period == 0 || values.len() < period {
        return None;
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut value = values[..period].iter().sum::<f64>() / period as f64;
    for price in &values[period..] {
        value = (price - value) * multiplier + value;
    }
    Some(value)
}

pub fn atr(candles: &[Candle], period: usize) -> Option<f64> {
    if period == 0 || candles.len() <= period {
        return None;
    }
    let start = candles.len() - period;
    let mut total = 0.0;
    for index in start..candles.len() {
        let previous_close = candles[index - 1].close;
        let candle = &candles[index];
        total += (candle.high - candle.low)
            .max((candle.high - previous_close).abs())
            .max((candle.low - previous_close).abs());
    }
    Some(total / period as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ema_uses_seeded_average() {
        let values: Vec<f64> = (1..=30).map(|v| v as f64).collect();
        assert!(ema(&values, 12).unwrap() > 24.0);
        assert_eq!(ema(&values[..3], 12), None);
    }
}
