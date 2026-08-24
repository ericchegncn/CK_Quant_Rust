# Architecture

## Latency-critical path

```text
Binance user-data stream ---> order/position event ---> in-memory active state
                                      |                         |
                                      v                         v
                              high-priority risk queue ---> order command
                                      |                         |
                                      +----> async WAL writer <-+

market candles ---> strategy worker ---> normal order queue
history/API/UI ---> bounded background reconciliation queue
```

The Tokio event loop uses biased selection. Risk exits are serviced before
strategy orders, and strategy orders before background work. Each order carries
a UUID client ID so a retry cannot silently create a duplicate position.

SQLite runs in WAL mode, but WAL alone is not the isolation boundary. Active
orders/checkpoints live in the configured database while terminal orders are
moved to a physically separate `*.history.sqlite` database. The trading loop
therefore queries a table bounded by current activity, not lifetime activity.
Dashboard, export and analytical queries must use the history database or an
incrementally maintained `account_stats` snapshot and must never run on a risk
or strategy lane. PostgreSQL can be added for fleets, but split SQLite remains
suitable for one bot when writes are serialized and queries are bounded.

This is a hard performance invariant: increasing terminal history from ten
thousand to one million records must not materially increase single-order
submission or persistence latency. CI verifies that terminal orders leave the
active database; release benchmarking will enforce P50/P95/P99 latency at
multiple history sizes.

## Compatibility boundary

Compatibility means the same concepts and outcomes, not Python source-level
plugins. Rust strategies implement `Strategy` with equivalents for candle
signals, `custom_exit`, leverage and startup candles. A future optional Python
bridge can run legacy strategies out of process, but production performance
comes from native Rust strategies.

## Live safety gates

A live build is blocked until all of these are verified:

1. Exchange symbol filters, price/amount precision and minimum notional.
2. Binance USD-M user-data WebSocket with reconnect and listen-key renewal.
3. REST reconciliation after sequence gaps without scanning full history.
4. Clock synchronization and `recvWindow` handling.
5. Isolated/cross margin, leverage tiers and liquidation buffer.
6. Stoploss-on-exchange replacement and emergency exit behavior.
7. API rate-limit budgets, exponential backoff and circuit breaking.
8. Crash recovery and duplicate-order fault injection.
9. P99 risk-order submission below 2 seconds under a synthetic burst.
10. Dry-run and shadow-mode parity against CK Quant on identical candles.
11. History-scaling test at 10k, 100k and 1m terminal orders with no material
    regression in the active order path.
