# dags/init_crm_database.py
#
# Initialises the CRM SQLite database used by the customer outreach exercise.
# Run this DAG once manually before triggering customer_outreach_pipeline.
#
# Assumes a connection with conn_id="crm_postgres" already exists in Airflow
# pointing at your SQLite database file.

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "customer_subscriptions"

with DAG(
    dag_id="init_database",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["setup"],
):

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS customers (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT    NOT NULL,
                tier      TEXT    NOT NULL CHECK(tier IN ('gold', 'silver', 'bronze')),
                is_active INTEGER NOT NULL DEFAULT 1
            );
        """,
    )

    seed_data = SQLExecuteQueryOperator(
        task_id="seed_data",
        conn_id=CONN_ID,
        sql="""
            INSERT OR IGNORE INTO customers (id, name, tier, is_active) VALUES
                (1,  'Alice',   'gold',   1),
                (2,  'Bob',     'gold',   1),
                (3,  'Carol',   'gold',   0),
                (4,  'Dave',    'silver', 1),
                (5,  'Eve',     'silver', 1),
                (6,  'Frank',   'silver', 0),
                (7,  'Grace',   'silver', 1),
                (8,  'Heidi',   'bronze', 1),
                (9,  'Ivan',    'bronze', 1),
                (10, 'Judy',    'bronze', 1),
                (11, 'Karl',    'bronze', 0),
                (12, 'Laura',   'bronze', 1);
        """,
    )

    create_table >> seed_data