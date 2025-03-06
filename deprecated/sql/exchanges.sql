-- exchanges.sql

CREATE TABLE IF NOT EXISTS exchanges
(
    id          INTEGER PRIMARY KEY,
    type        TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    locale      TEXT NOT NULL,
    name        TEXT NOT NULL,
    url         TEXT
);
