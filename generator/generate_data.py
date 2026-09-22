"""
Генератор синтетических данных интернет-магазина.

Идея: при первом запуске создаём "базу" пользователей и товаров (справочники),
а при каждом следующем запуске (за конкретную batch_date) генерируем
новую порцию заказов и кликстрим-событий за этот день, плюс иногда
подмешиваем новых пользователей — как в реальном бизнесе.

Запуск:
    python generate_data.py --batch-date 2024-01-15 --n-users-new 50 --n-events 2000
"""

import argparse
import os
import random
from datetime import datetime, timedelta

import numpy as np
import psycopg2
from faker import Faker

fake = Faker()

CATEGORIES = ["electronics", "home", "beauty", "sports", "books", "toys"]
ACQUISITION_CHANNELS = ["organic", "ads", "referral", "email"]
COUNTRIES = ["US", "DE", "FR", "GB", "PL", "ES"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Веса, чтобы распределение не было равномерным (реалистичнее для аналитики)
ACQUISITION_WEIGHTS = [0.5, 0.3, 0.15, 0.05]
CATEGORY_WEIGHTS = [0.30, 0.20, 0.15, 0.15, 0.12, 0.08]


def get_conn(dsn: str):
    return psycopg2.connect(dsn)


def ensure_products(conn, n_products: int = 200):
    """Создаёт каталог товаров один раз, если он ещё пуст."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products;")
        (count,) = cur.fetchone()
        if count > 0:
            return
        rows = []
        for _ in range(n_products):
            category = np.random.choice(CATEGORIES, p=CATEGORY_WEIGHTS)
            price = round(np.random.lognormal(mean=3.2, sigma=0.9), 2)
            price = max(1.0, min(price, 2000.0))
            rows.append((fake.catch_phrase()[:120], category, price))
        cur.executemany(
            "INSERT INTO products (product_name, category, price) VALUES (%s, %s, %s);",
            rows,
        )
    conn.commit()
    print(f"[generator] создано {n_products} товаров")


def add_new_users(conn, n_new_users: int, batch_date: datetime.date):
    with conn.cursor() as cur:
        rows = []
        for _ in range(n_new_users):
            country = random.choice(COUNTRIES)
            age = int(np.clip(np.random.normal(34, 10), 16, 75))
            channel = np.random.choice(ACQUISITION_CHANNELS, p=ACQUISITION_WEIGHTS)
            rows.append((batch_date, country, age, channel))
        cur.executemany(
            "INSERT INTO users (signup_date, country, age, acquisition_ch) "
            "VALUES (%s, %s, %s, %s);",
            rows,
        )
    conn.commit()
    print(f"[generator] добавлено {n_new_users} новых пользователей на {batch_date}")


def get_user_and_product_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users;")
        user_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT product_id, price FROM products;")
        products = cur.fetchall()  # [(id, price), ...]
    return user_ids, products


def generate_events_and_orders(conn, batch_date: datetime.date, n_events: int):
    """
    Генерирует воронку событий за день: view -> (иногда) add_to_cart -> (иногда) purchase.
    Часть purchase-событий превращаем в реальные заказы (orders + order_items),
    чтобы orders были согласованы с events.
    """
    user_ids, products = get_user_and_product_ids(conn)
    if not user_ids or not products:
        raise RuntimeError("Нет пользователей или товаров — сначала создай справочники")

    # 20% активных пользователей дают 80% событий (типичный паттерн)
    power_users = np.random.choice(
        user_ids, size=max(1, len(user_ids) // 5), replace=False
    )

    events_rows = []
    orders_to_create = []  # (user_id, [(product_id, price), ...])

    day_start = datetime.combine(batch_date, datetime.min.time())

    for _ in range(n_events):
        if random.random() < 0.8 and len(power_users) > 0:
            user_id = int(np.random.choice(power_users))
        else:
            user_id = int(np.random.choice(user_ids))

        product_id, price = random.choice(products)
        event_ts = day_start + timedelta(seconds=random.randint(0, 86399))

        # Воронка: view почти всегда, cart реже, purchase ещё реже
        r = random.random()
        if r < 0.65:
            event_type = "view"
        elif r < 0.90:
            event_type = "add_to_cart"
        else:
            event_type = "purchase"
            orders_to_create.append((user_id, product_id, price, event_ts))

        events_rows.append((user_id, product_id, event_type, event_ts, batch_date))

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO events (user_id, product_id, event_type, event_ts, load_batch_date) "
            "VALUES (%s, %s, %s, %s, %s);",
            events_rows,
        )

        # Группируем purchase-события в заказы (1 заказ = 1..3 позиции одного юзера)
        random.shuffle(orders_to_create)
        i = 0
        orders_created = 0
        while i < len(orders_to_create):
            chunk_size = random.randint(1, min(3, len(orders_to_create) - i))
            chunk = orders_to_create[i : i + chunk_size]
            i += chunk_size
            user_id = chunk[0][0]
            order_ts = chunk[0][3]

            status = np.random.choice(
                ["completed", "cancelled", "refunded"], p=[0.90, 0.06, 0.04]
            )

            cur.execute(
                "INSERT INTO orders (user_id, order_date, status, load_batch_date) "
                "VALUES (%s, %s, %s, %s) RETURNING order_id;",
                (user_id, order_ts, status, batch_date),
            )
            (order_id,) = cur.fetchone()

            for _, product_id, price, _ in chunk:
                qty = np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1])
                cur.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s);",
                    (order_id, product_id, int(qty), price),
                )
            orders_created += 1

        cur.execute(
            "INSERT INTO pipeline_runs (run_date, status) VALUES (%s, 'generated') "
            "ON CONFLICT (run_date) DO UPDATE SET status = 'generated', generated_at = now();",
            (batch_date,),
        )

    conn.commit()
    print(
        f"[generator] batch {batch_date}: {len(events_rows)} событий, "
        f"{orders_created} заказов"
    )


def build_default_dsn() -> str:
    """Собирает DSN из переменных окружения (заданы в docker-compose.yml),
    чтобы пароль/хост задавались в одном месте и не расходились по файлам."""
    host = os.environ["PGHOST"]
    port = os.environ["PGPORT"]
    dbname = os.environ["PGDATABASE"]
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    return f"dbname={dbname} user={user} password={password} host={host} port={port}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--n-users-new", type=int, default=20)
    parser.add_argument("--n-events", type=int, default=1500)
    parser.add_argument("--dsn", default=None, help="Если не задан — берётся из переменных окружения PG*")
    args = parser.parse_args()

    # Validate required environment variables
    required_env_vars = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [var for var in required_env_vars if var not in os.environ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in .env or export before running. See .env.example for reference."
        )

    dsn = args.dsn or build_default_dsn()
    batch_date = datetime.strptime(args.batch_date, "%Y-%m-%d").date()

    conn = get_conn(dsn)
    try:
        ensure_products(conn)
        add_new_users(conn, args.n_users_new, batch_date)
        generate_events_and_orders(conn, batch_date, args.n_events)
    finally:
        conn.close()


if __name__ == "__main__":
    main()