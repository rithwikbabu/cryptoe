-- conditions.sql

CREATE TABLE IF NOT EXISTS conditions
(
    id          INTEGER PRIMARY KEY,
    type        TEXT NOT NULL,
    name        TEXT NOT NULL,
    description TEXT,
    asset_class TEXT NOT NULL,
    data_types  TEXT[]
);
