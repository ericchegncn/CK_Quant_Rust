# CK Quant Rust

CK Quant Rust is a clean-room, low-latency trading engine that is being built
for behavioral compatibility with the configuration and strategy lifecycle of
Freqtrade. It is not yet a drop-in replacement for all Freqtrade features.

The first milestone targets the latency-sensitive live path: isolated priority
queues for risk exits, strategy orders and background reconciliation; bounded
active-order state; SQLite WAL persistence; idempotent client order IDs; and
per-order queue/network latency logs.

## Privacy boundary

The owner's CK_Trend strategies and production `config_15m.json` are private.
They are never part of this repository or its Docker build context. Public
builds contain only `SampleEmaStrategy` and a credential-free example config.
The `private/`, `user_data/`, `data/`, `secrets/`, `config*.json` and
`CK_Trend*` patterns are excluded by both `.gitignore` and `.dockerignore`.

Before publishing, run:

```powershell
pwsh scripts/privacy-check.ps1
```

## Current commands

```console
ck-quant-rust validate --config config.example.json
ck-quant-rust serve --config config.example.json --listen 127.0.0.1:8080
ck-quant-rust backtest --config config.example.json --candles candles.csv --pair BTC/USDT:USDT
```

`serve` is paper-only in milestone 1. Live Binance order submission and the
2026 routed user-data WebSocket exist as connector modules but remain
deliberately disabled in the CLI until account-position reconciliation,
exchange filters, precision handling, clock drift, liquidation protection and
failover tests all pass.

## Why a rewrite does not automatically remove a 30-second delay

Language runtime is only one component. During volatility, synchronous REST
order polling, exchange rate limits/retries, database locks and notification/UI
work can dominate latency. CK Quant Rust therefore gives exits their own
priority lane and keeps history reconciliation out of the critical loop.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/ROADMAP.md](docs/ROADMAP.md).

## License and attribution

GPL-3.0-only. This implementation uses documented behavior and public APIs; it
does not copy the private CK_Trend source into public artifacts.
