-- trades.sql
CREATE TABLE IF NOT EXISTS trades
(
    trade_id     INTEGER          NOT NULL,
    ticker       TEXT             NOT NULL,
    condition_id INTEGER          NOT NULL,
    exchange_id  INTEGER          NOT NULL,
    timestamp    BIGINT           NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    size         DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (trade_id, ticker),
    FOREIGN KEY (condition_id) REFERENCES conditions (id),
    FOREIGN KEY (exchange_id) REFERENCES exchanges (id),
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

CREATE INDEX IF NOT EXISTS idx_trades_ticker_timestamp ON trades (ticker, timestamp);

-- trades_daily_ohlcv
CREATE MATERIALIZED VIEW trades_hour_ohlcv AS
SELECT ticker,
       DATE_TRUNC('hour', TO_TIMESTAMP(timestamp / 1e9) AT TIME ZONE 'UTC') AS timestamp,
       (ARRAY_AGG(price ORDER BY timestamp ASC))[1]                        AS open,
       MAX(price)                                                          AS high,
       MIN(price)                                                          AS low,
       (ARRAY_AGG(price ORDER BY timestamp DESC))[1]                       AS close,
       SUM(size)                                                           AS volume
FROM trades
GROUP BY ticker, timestamp;

CREATE INDEX IF NOT EXISTS trades_hour_ohlcv_idx ON trades_hour_ohlcv (ticker, timestamp);

-- trades_minute_ohlcv
CREATE MATERIALIZED VIEW trades_minute_ohlcv AS
SELECT ticker,
       DATE_TRUNC('minute', TO_TIMESTAMP(timestamp / 1e9) AT TIME ZONE 'UTC') AS timestamp,
       (ARRAY_AGG(price ORDER BY timestamp ASC))[1]                        AS open,
       MAX(price)                                                          AS high,
       MIN(price)                                                          AS low,
       (ARRAY_AGG(price ORDER BY timestamp DESC))[1]                       AS close,
       SUM(size)                                                           AS volume
FROM trades
GROUP BY ticker, timestamp;

CREATE INDEX IF NOT EXISTS trades_minute_ohlcv_idx ON trades_minute_ohlcv (ticker, timestamp);

-- https://chatgpt.com/share/67c8a09e-d048-8008-acf0-d7bfc5745bac