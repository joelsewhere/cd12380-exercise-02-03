# dags/customer_outreach_pipeline_solution.py
#
# Airflow 3.1 Exercise — Dynamic Task Mapping with SQLExecuteQueryOperator
# (SOLUTION)

from __future__ import annotations

from datetime import datetime
from collections import defaultdict

from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONN_ID = "customer_subscriptions"
OUTREACH_TEMPLATES = {
    "gold":   "You are a valued Gold member — here is your early access link: https://example.com/gold/{customer_id}",
    "silver": "Upgrade to Gold and unlock exclusive benefits: https://example.com/upgrade/{customer_id}",
    "bronze": "Here is a special offer just for you: https://example.com/offer/{customer_id}",
}


# ---------------------------------------------------------------------------
# Helper (do not modify)
# ---------------------------------------------------------------------------

def _send_message(customer_id: int, message: str) -> None:
    """Simulate sending an outreach message to a customer."""
    print(f"  → customer {customer_id}: {message}")


# ---------------------------------------------------------------------------
# EXERCISE 1 SOLUTION — output_processor function
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="customer_outreach_pipeline",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise", "dynamic-mapping"],
):

    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        conn_id=CONN_ID,
        sql="""
            SELECT id, tier
            FROM customers
            WHERE is_active = TRUE
            ORDER BY tier, id
        """,
    )

    @task
    def group_by_tier(results: list) -> list[dict]:
        grouped = defaultdict(list)
        for customer_id, tier in results:
            grouped[tier].append(customer_id)
        return [
            {"tier": tier, "customer_ids": customer_ids}
            for tier, customer_ids in grouped.items()
        ]

    # -----------------------------------------------------------------------
    # EXERCISE 2 SOLUTION — dynamic task with map_index_template
    # -----------------------------------------------------------------------

    @task(map_index_template="{{ task.op_kwargs['group']['tier'] }}")
    def send_outreach(group: dict) -> None:
        tier         = group["tier"]
        customer_ids = group["customer_ids"]
        template     = OUTREACH_TEMPLATES[tier]

        print(f"Processing tier: {tier} ({len(customer_ids)} customers)")
        for customer_id in customer_ids:
            message = template.format(customer_id=customer_id)
            _send_message(customer_id, message)

    # -----------------------------------------------------------------------
    # EXERCISE 3 SOLUTION — wire the dynamic expansion
    # -----------------------------------------------------------------------
    groups = group_by_tier(fetch_customers.output)
    send_outreach.expand(group=groups)