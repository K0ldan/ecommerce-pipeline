# E-commerce Analytics Pipeline

Ежедневный ETL-пайплайн для синтетического интернет-магазина.
Стек: **Python, PostgreSQL, Spark, Airflow, pandas, ClickHouse**.

## Архитектура

```
generate_data.py (Faker/numpy)
        │  пишет заказы/события за batch_date
        ▼
PostgreSQL (OLTP — "продакшн" магазин)
        │
        ▼
validate.py (pandas data quality checks)
        │  падает и останавливает DAG при проблемах
        ▼
transform_orders.py (Spark: JDBC read, cleaning, join)
        │  parquet-слой (order_lines, events)
        ▼
build_marts.py (Spark: агрегаты)
        │
        ▼
ClickHouse (аналитические витрины)
        │
        ▼
notebooks/eda.py — визуализация метрик
```

Оркестрация: Airflow DAG `ecommerce_pipeline`
(`generate_data → data_quality_check → spark_transform → spark_build_marts`), расписание `@daily`.

## Быстрый старт

```bash
git clone <this-repo>
cd ecommerce-pipeline

cp .env.example .env
# отредактируйте .env (пароли БД, ClickHouse, Airflow)

docker compose up --build -d
```

После поднятия сервисов:

1. Airflow UI: http://localhost:8080 (логин/пароль из `.env`)
2. Включить DAG `ecommerce_pipeline`
3. Запустить вручную на нескольких датах
4. Проверить витрины в ClickHouse:
   ```bash
   docker exec -it ecommerce-pipeline-clickhouse-1 clickhouse-client
   SELECT * FROM ecommerce.daily_sales_by_category ORDER BY sales_date;
   ```
5. EDA (локально):
   ```bash
   pip install clickhouse-connect pandas matplotlib
   python notebooks/eda.py
   ```

## Переменные окружения (`.env`)

| Переменная | Описание |
|------------|----------|
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Meta-BD Airflow (postgresql://...) |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Flask secret (≥32 символа) |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Бизнес-Постгрес |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | ClickHouse |
| `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD` | Админ Airflow |

Шаблон: `.env.example`.

## Структура

```
ecommerce-pipeline/
├── docker-compose.yml
├── docker/airflow.Dockerfile
├── requirements.txt
├── dags/ecommerce_pipeline_dag.py
├── generator/generate_data.py
├── spark_jobs/
│   ├── transform_orders.py
│   └── build_marts.py
├── quality_checks/validate.py
├── sql/
│   ├── postgres_ddl.sql
│   └── clickhouse_ddl.sql
└── notebooks/eda.py
```

## Идемпотентность

Каждый запуск работает с конкретной `batch_date` (`{{ ds }}`):
- Генератор пишет данные только за эту дату
- Spark перезаписывает parquet-партиции (`overwrite`)
- ClickHouse использует `ReplacingMergeTree`
- `pipeline_runs` в Postgres фиксирует статус через `ON CONFLICT`

Повторный запуск за тот же день не создаёт дублей.