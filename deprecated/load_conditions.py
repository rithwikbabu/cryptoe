import os

import psycopg2
from polygon import RESTClient
from polygon.rest.models import Condition
from psycopg2.extras import execute_values


def fetch_conditions(api_key: str, asset_class: str = 'crypto') -> list[Condition]:
    """Fetch conditions data for the given asset class from Polygon."""
    client = RESTClient(api_key=api_key)
    return list(client.list_conditions(asset_class=asset_class))


def insert_conditions(values: list[tuple], conn_string: str) -> None:
    """Bulk insert condition records into the database."""
    query = """
    INSERT INTO conditions (id, type, name, description, asset_class, data_types)
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

    # Fetch conditions and prepare values for insertion
    conditions: list[Condition] = fetch_conditions(polygon_api_key)
    values = [
        (co.id, co.type, co.name, co.description, co.asset_class, co.data_types)
        for co in conditions
    ]

    # Define your PostgreSQL connection string
    conn_string = "dbname=postgres user=postgres password=mypass host=localhost"
    insert_conditions(values, conn_string)

    print("Conditions data inserted successfully.")


if __name__ == '__main__':
    main()
