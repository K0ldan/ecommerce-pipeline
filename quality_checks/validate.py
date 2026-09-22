"""
Data quality проверки за конкретный batch_date.
Читает данные напрямую из Postgres через pandas и падает (raise),
если найдены критичные проблемы — это остановит DAG в Airflow.

Запуск:
    python validate.py --batch-date 2024-01-15
"""

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine


def build_default_dsn() -> str:
    host = os.environ["PGHOST"]
    port = os.environ["PGPORT"]
    dbname = os.environ["PGDATABASE"]
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def validate_env_vars():
    """Validate required environment variables are set."""
    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in .env or export before running. See .env.example for reference."
        )


validate_env_vars()
DSN = build_default_dsn()


class DataQualityError(Exception):
    pass


def check_no_nulls(df: pd.DataFrame, columns: list[str], name: str):
    for col in columns:
        n_nulls = df[col].isna().sum()
        if n_nulls > 0:
            raise DataQualityError(f"[{name}] {n_nulls} NULL значений в колонке {col}")


def check_no_duplicates(df: pd.DataFrame, key: str, name: str):
    n_dupes = df[key].duplicated().sum()
    if n_dupes > 0:
        raise DataQualityError(f"[{name}] {n_dupes} дублей по ключу {key}")


def check_positive(df: pd.DataFrame, column: str, name: str):
    n_bad = (df[column] <= 0).sum()
    if n_bad > 0:
        raise DataQualityError(f"[{name}] {n_bad} строк с {column} <= 0")


def check_referential_integrity(child_df, child_key, parent_ids, name):
    orphans = ~child_df[child_key].isin(parent_ids)
    n_orphans = orphans.sum()
    if n_orphans > 0:
        raise DataQualityError(
            f"[{name}] {n_orphans} строк ссылаются на несуществующий {child_key}"
        )


def check_anomaly_volume(df: pd.DataFrame, name: str, min_expected: int = 1):
    if len(df) < min_expected:
        raise DataQualityError(
            f"[{name}] подозрительно мало строк за батч: {len(df)} (ожидалось >= {min_expected})"
        )


def main(batch_date: str):
    engine = create_engine(DSN)

    orders = pd.read_sql(
        "SELECT * FROM orders WHERE load_batch_date = %(d)s", engine, params={"d": batch_date}
    )
    order_items = pd.read_sql(
        "SELECT oi.* FROM order_items oi "
        "JOIN orders o ON o.order_id = oi.order_id "
        "WHERE o.load_batch_date = %(d)s",
        engine,
        params={"d": batch_date},
    )
    events = pd.read_sql(
        "SELECT * FROM events WHERE load_batch_date = %(d)s", engine, params={"d": batch_date}
    )
    users = pd.read_sql("SELECT user_id FROM users", engine)
    products = pd.read_sql("SELECT product_id FROM products", engine)

    errors = []
    checks = [
        (check_anomaly_volume, (orders, "orders")),
        (check_anomaly_volume, (events, "events", 1)),
        (check_no_nulls, (orders, ["user_id", "order_date", "status"], "orders")),
        (check_no_duplicates, (orders, "order_id", "orders")),
        (check_positive, (order_items, "quantity", "order_items")),
        (
            check_referential_integrity,
            (orders, "user_id", set(users["user_id"]), "orders->users"),
        ),
        (
            check_referential_integrity,
            (events, "user_id", set(users["user_id"]), "events->users"),
        ),
    ]

    for fn, args in checks:
        try:
            fn(*args)
        except DataQualityError as e:
            errors.append(str(e))

    if errors:
        print("DATA QUALITY FAILED:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    print(f"[quality] batch {batch_date}: все проверки пройдены "
          f"({len(orders)} заказов, {len(order_items)} позиций, {len(events)} событий)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    main(args.batch_date)