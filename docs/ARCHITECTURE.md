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

SQLite runs in WAL mode with an index on `(status, pair, updated_at)`. The live
loop never scans closed historical orders. PostgreSQL can be added for fleets,
but SQLite remains suitable for a single bot when writes are serialized and
queries are bounded.

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

