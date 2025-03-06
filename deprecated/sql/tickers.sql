-- tickers.sql

CREATE TABLE IF NOT EXISTS tickers
(
    ticker               TEXT PRIMARY KEY,
    name                 TEXT        NOT NULL,
    market               TEXT        NOT NULL,
    locale               TEXT        NOT NULL,
    active               BOOLEAN     NOT NULL,
    currency_symbol      TEXT        NOT NULL,
    currency_name        TEXT        NOT NULL,
    base_currency_symbol TEXT        NOT NULL,
    base_currency_name   TEXT        NOT NULL,
    last_updated_utc     TIMESTAMPTZ NOT NULL
);
