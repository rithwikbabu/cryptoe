import os

import psycopg2
from polygon import RESTClient
from polygon.rest.models import Exchange
from psycopg2.extras import execute_values


def fetch_exchanges(api_key: str, asset_class: str = 'crypto') -> list[Exchange]:
    """Fetch exchanges data for the given asset class from Polygon."""
    client = RESTClient(api_key=api_key)
    return client.get_exchanges(asset_class=asset_class)


def insert_exchanges(values: list[tuple], conn_string: str) -> None:
    """Bulk insert exchange records into the database."""
    query = """
    INSERT INTO exchanges (id, type, asset_class, locale, name, url)
    VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()


def main():
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    if not polygon_api_key:
        raise ValueError("Polygon API key not set in environment variable 'POLYGON_API_KEY'")

    # Fetch exchanges and prepare values for insertion
    exchanges: list[Exchange] = fetch_exchanges(polygon_api_key)
    values = [
        (ex.id, ex.type, ex.asset_class, ex.locale, ex.name, ex.url)
        for ex in exchanges
    ]

    # Define your PostgreSQL connection string
    conn_string = "dbname=postgres user=postgres password=mypass host=localhost"
    insert_exchanges(values, conn_string)

    print("Exchanges data inserted successfully.")


if __name__ == '__main__':
    main()
