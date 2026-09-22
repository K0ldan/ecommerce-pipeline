"""
Spark-джоба: читает очищенный parquet-слой (order_lines, events) за batch_date,
считает агрегаты и пишет их в ClickHouse через clickhouse-connect (batch insert
через pandas, т.к. родного Spark-коннектора для ClickHouse в MVP не поднимаем —
это осознанное упрощение, см. README, раздел "Trade-offs").

Запуск:
    spark-submit build_marts.py --batch-date 2024-01-15
"""

import argparse
import os

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CH_HOST = os.environ["CLICKHOUSE_HOST"]
CH_PORT = int(os.environ["CLICKHOUSE_PORT"])
CH_USER = os.environ["CLICKHOUSE_USER"]
CH_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]


def validate_env_vars():
    """Validate required environment variables are set."""
    required = ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in .env or export before running. See .env.example for reference."
        )


validate_env_vars()


def build_spark():
    return SparkSession.builder.appName("ecommerce_build_marts").getOrCreate()


def write_to_clickhouse(df_pandas, table: str):
    if df_pandas.empty:
        print(f"[marts] {table}: нет данных для записи, пропускаю")
        return
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )
    client.insert_df(f"ecommerce.{table}", df_pandas)
    print(f"[marts] записано {len(df_pandas)} строк в ecommerce.{table}")


def main(batch_date: str, input_path: str):
    spark = build_spark()

    order_lines = spark.read.parquet(f"{input_path}/order_lines/{batch_date}")
    events = spark.read.parquet(f"{input_path}/events/{batch_date}")

    # --- Витрина 1: продажи по категориям за день ---
    daily_sales = (
        order_lines.groupBy(F.to_date(F.lit(batch_date)).alias("sales_date"), "category")
        .agg(
            F.countDistinct("order_id").alias("orders_cnt"),
            F.sum("quantity").alias("items_cnt"),
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
        )
        .withColumn(
            "avg_order_value", F.round(F.col("revenue") / F.col("orders_cnt"), 2)
        )
    )

    # --- Витрина 2: воронка событий за день ---
    funnel_counts = events.groupBy("event_type").count().toPandas().set_index("event_type")["count"]
    views = int(funnel_counts.get("view", 0))
    carts = int(funnel_counts.get("add_to_cart", 0))
    purchases = int(funnel_counts.get("purchase", 0))
    import pandas as pd

    daily_funnel = pd.DataFrame(
        [
            {
                "event_date": pd.to_datetime(batch_date).date(),
                "views": views,
                "add_to_cart": carts,
                "purchases": purchases,
                "view_to_cart_rate": round(carts / views, 4) if views else 0.0,
                "cart_to_purchase_rate": round(purchases / carts, 4) if carts else 0.0,
            }
        ]
    )

    # --- Витрина 3: топ товаров (по этому батчу; в проде считали бы кумулятивно) ---
    top_products = (
        order_lines.groupBy("product_id", "product_name", "category")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_qty"),
        )
        .withColumn("calc_date", F.to_date(F.lit(batch_date)))
        .orderBy(F.desc("total_revenue"))
        .limit(50)
    )

    # --- Витрина 4: LTV по когортам (месяц регистрации x канал привлечения) ---
    ltv_cohorts = (
        order_lines.withColumn("signup_month", F.date_trunc("month", "signup_date"))
        .groupBy("signup_month", "acquisition_ch")
        .agg(
            F.countDistinct("user_id").alias("users_cnt"),
            F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        )
        .withColumn(
            "avg_ltv", F.round(F.col("total_revenue") / F.col("users_cnt"), 2)
        )
        .withColumn("calc_date", F.to_date(F.lit(batch_date)))
    )

    write_to_clickhouse(daily_sales.toPandas(), "daily_sales_by_category")
    write_to_clickhouse(daily_funnel, "daily_funnel")
    write_to_clickhouse(top_products.toPandas(), "top_products")
    write_to_clickhouse(
        ltv_cohorts.select(
            "calc_date", "signup_month", "acquisition_ch", "users_cnt",
            "total_revenue", "avg_ltv",
        ).toPandas(),
        "user_ltv_cohorts",
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--input-path", default="/opt/data/clean")
    args = parser.parse_args()
    main(args.batch_date, args.input_path)