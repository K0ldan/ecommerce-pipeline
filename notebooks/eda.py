"""
Лёгкий EDA поверх витрин ClickHouse. Можно запускать как скрипт
или построчно вставить в Jupyter-ноутбук.

Требует: pip install clickhouse-connect pandas matplotlib python-dotenv

Переменные окружения (из .env):
- CLICKHOUSE_HOST
- CLICKHOUSE_PORT
- CLICKHOUSE_USER
- CLICKHOUSE_PASSWORD
"""

import os
import clickhouse_connect
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

required_vars = ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"]
missing = [var for var in required_vars if var not in os.environ]
if missing:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing)}. "
        f"Set them in .env or export before running. See .env.example for reference."
    )

client = clickhouse_connect.get_client(
    host=os.environ["CLICKHOUSE_HOST"],
    port=int(os.environ["CLICKHOUSE_PORT"]),
    username=os.environ["CLICKHOUSE_USER"],
    password=os.environ["CLICKHOUSE_PASSWORD"],
)

# 1. Динамика выручки по дням
revenue_by_day = client.query_df(
    """
    SELECT sales_date, sum(revenue) AS revenue
    FROM ecommerce.daily_sales_by_category
    GROUP BY sales_date
    ORDER BY sales_date
    """
)
print(revenue_by_day)

revenue_by_day.plot(x="sales_date", y="revenue", kind="line", marker="o", title="Revenue by day")
plt.tight_layout()
plt.savefig("revenue_by_day.png")

# 2. Топ категорий по суммарной выручке
top_categories = client.query_df(
    """
    SELECT category, sum(revenue) AS revenue
    FROM ecommerce.daily_sales_by_category
    GROUP BY category
    ORDER BY revenue DESC
    """
)
print(top_categories)

# 3. Конверсия воронки за последний день
funnel = client.query_df(
    "SELECT * FROM ecommerce.daily_funnel ORDER BY event_date DESC LIMIT 1"
)
print(funnel)

# 4. LTV по каналам привлечения
ltv = client.query_df(
    """
    SELECT acquisition_ch, avg(avg_ltv) AS avg_ltv, sum(users_cnt) AS users
    FROM ecommerce.user_ltv_cohorts
    GROUP BY acquisition_ch
    ORDER BY avg_ltv DESC
    """
)
print(ltv)
