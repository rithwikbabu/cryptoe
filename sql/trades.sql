-- trades.sql

CREATE TABLE IF NOT EXISTS trades
(
    ticker                TEXT             NOT NULL,
    condition_id          INTEGER,
    exchange_id           INTEGER,
    trade_id              INTEGER,
    participant_timestamp TIMESTAMPTZ      NOT NULL,
    price                 DOUBLE PRECISION NOT NULL,
    size                  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, trade_id, participant_timestamp),
    FOREIGN KEY (condition_id) REFERENCES conditions (id),
    FOREIGN KEY (exchange_id) REFERENCES exchanges (id),
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

-- Convert the 'trades' table into a TimescaleDB hypertable
SELECT create_hypertable('trades', 'participant_timestamp', if_not_exists => TRUE);
