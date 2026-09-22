"""
Spark-джоба: читает "сырые" данные заказов/событий за конкретный batch_date
из Postgres, чистит и валидирует, кладёт очищенный parquet-слой на диск
(в реальном проекте — в S3/HDFS/data lake).

Запуск:
    spark-submit transform_orders.py --batch-date 2024-01-15
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PGHOST = os.environ["PGHOST"]
PGPORT = os.environ["PGPORT"]
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

JDBC_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
JDBC_PROPS = {"user": PGUSER, "password": PGPASSWORD, "driver": "org.postgresql.Driver"}


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


def read_table(spark, table: str):
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)


def build_spark():
    # postgresql-42.7.3.jar уже лежит в /opt/spark/jars (кладётся при сборке
    # Docker-образа), поэтому Ivy-резолюция через spark.jars.packages не нужна —
    # она лишь замедляет (или подвешивает) запуск, полезая в сеть каждый раз.
    return SparkSession.builder.appName("ecommerce_transform_orders").getOrCreate()


def main(batch_date: str, output_path: str):
    spark = build_spark()

    orders = read_table(spark, "orders").filter(F.col("load_batch_date") == batch_date)
    order_items = read_table(spark, "order_items")
    products = read_table(spark, "products")
    events = read_table(spark, "events").filter(F.col("load_batch_date") == batch_date)
    users = read_table(spark, "users")

    # --- Очистка / валидация ---
    orders_clean = orders.filter(F.col("status") != "cancelled").dropDuplicates(["order_id"])

    order_items_clean = order_items.filter(
        (F.col("quantity") > 0) & (F.col("unit_price") >= 0)
    ).dropDuplicates(["order_item_id"])

    # --- Обогащение: заказы + позиции + товары + пользователи ---
    order_lines = (
        orders_clean.alias("o")
        .join(order_items_clean.alias("oi"), "order_id")
        .join(products.alias("p"), "product_id")
        .join(users.alias("u"), "user_id")
        .withColumn("line_revenue", F.col("oi.quantity") * F.col("oi.unit_price"))
        .select(
            "order_id",
            "o.order_date",
            "user_id",
            "u.country",
            "u.acquisition_ch",
            "u.signup_date",
            "product_id",
            "p.category",
            "p.product_name",
            "oi.quantity",
            "oi.unit_price",
            "line_revenue",
        )
    )

    events_clean = events.dropDuplicates(["event_id"])

    order_lines.write.mode("overwrite").parquet(f"{output_path}/order_lines/{batch_date}")
    events_clean.write.mode("overwrite").parquet(f"{output_path}/events/{batch_date}")

    print(
        f"[spark] batch {batch_date}: {order_lines.count()} строк заказов, "
        f"{events_clean.count()} событий записано в {output_path}"
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--output-path", default="/opt/data/clean")
    args = parser.parse_args()
    main(args.batch_date, args.output_path)