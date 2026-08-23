# Compatibility roadmap

## M1 - Core engine (current)

- Freqtrade-like JSON5 config loader with unknown-field preservation.
- Domain model, strategy trait, sample strategy and basic CSV runner.
- Three priority lanes with risk-first scheduling.
- Paper exchange, Binance signed order connector, SQLite WAL storage.
- Health API, Docker build, tests and privacy guard.

## M2 - Binance shadow mode

- USD-M user-data stream: routed private endpoint, listen-key renewal, ping/pong,
  reconnect backoff and owned-order event parsing are implemented and unit tested.
- Market candle stream and account-position event parsing remain pending.
- Complete symbol metadata/precision and position reconciliation.
- Run beside CK Quant without submitting orders; compare every signal, leverage,
  exit reason and intended price.

## M3 - Controlled dry-run and live pilot

- Freqtrade-compatible wallets, pairlists, protections, stoploss, DCA and
  Telegram/API surfaces.
- Chaos tests for disconnects, rate limits, duplicate events and database
  contention.
- One-pair isolated-margin pilot with exchange-side stoploss.

## M4 - Broader Freqtrade parity

- Detailed backtesting, hyperoptimization, data download, WebUI and plugin APIs.
- Additional exchanges behind the `Exchange` trait.
- FreqAI-equivalent ML integration as an optional service.

“Complete Freqtrade replacement” is the M4 outcome, not the M1 label.
