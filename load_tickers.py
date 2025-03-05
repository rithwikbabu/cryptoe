import os

import psycopg2
from polygon import RESTClient
from polygon.rest.models import Ticker
from psycopg2.extras import execute_values


def fetch_tickers(api_key: str, market: str = 'crypto') -> list[Ticker]:
    """Fetch tickers data for the given market from Polygon."""
    client = RESTClient(api_key=api_key)
    return list(client.list_tickers(market=market))


def insert_conditions(values: list[tuple], conn_string: str) -> None:
    """Bulk insert condition records into the database."""
    query = """
    INSERT INTO tickers (ticker, name, market, locale, active, currency_symbol, currency_name, base_currency_symbol, base_currency_name, last_updated_utc)
    VALUES %s
    ON CONFLICT (ticker) DO NOTHING;
    """
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()


def main():
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    if not polygon_api_key:
        raise ValueError("Polygon API key not set in environment variable 'POLYGON_API_KEY'")

    # Fetch tickers and prepare values for insertion
    tickers: list[Ticker] = fetch_tickers(polygon_api_key)

    values = [
        (ti.ticker, ti.name, ti.market, ti.locale, ti.active, ti.currency_symbol, ti.currency_name,
         ti.base_currency_symbol, ti.base_currency_name, ti.last_updated_utc)
        for ti in tickers
    ]

    # Define your PostgreSQL connection string
    conn_string = "dbname=postgres user=postgres password=mypass host=localhost"
    insert_conditions(values, conn_string)

    print("Conditions data inserted successfully.")


if __name__ == '__main__':
    main()
