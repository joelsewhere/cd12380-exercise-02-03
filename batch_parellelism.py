# dags/customer_outreach_pipeline.py
#
# Airflow 3.1 Exercise — Dynamic Task Mapping with SQLExecuteQueryOperator
#
# Scenario
# --------
# A CRM database holds a `customers` table. Each customer belongs to a
# subscription tier (gold, silver, bronze). This pipeline queries all active
# customer IDs, groups them by tier, then processes each tier in parallel
# using dynamically mapped tasks.
#
# Database schema (read-only reference — do not create this yourself):
#
#   customers
#   ┌─────────────┬──────────────┬────────┬────────────┐
#   │ id (int PK) │ name (text)  │ tier   │ is_active  │
#   └─────────────┴──────────────┴────────┴────────────┘
#   tier values: 'gold' | 'silver' | 'bronze'
#
# Note: downstream tasks operate on customer IDs only — names are not
# selected or passed through this pipeline.
#
# Read README.md before editing this file.

from __future__ import annotations

from datetime import datetime
from collections import defaultdict

from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ---------------------------------------------------------------------------
# Constants (do not modify)
# ---------------------------------------------------------------------------

CONN_ID = "customer_subscriptions"
OUTREACH_TEMPLATES = {
    "gold":   "You are a valued Gold member — here is your early access link: https://example.com/gold/{customer_id}",
    "silver": "Upgrade to Gold and unlock exclusive benefits: https://example.com/upgrade/{customer_id}",
    "bronze": "Here is a special offer just for you: https://example.com/offer/{customer_id}",
}


# ---------------------------------------------------------------------------
# Helper (already implemented — do not modify)
# ---------------------------------------------------------------------------

def _send_message(customer_id: int, message: str) -> None:
    """Simulate sending an outreach message to a customer."""
    print(f"  → customer {customer_id}: {message}")


# ---------------------------------------------------------------------------
# EXERCISE 1 — write the output_processor function
#
# SQLExecuteQueryOperator accepts an `output_processor` callable. Airflow
# calls it with the raw query results: a list of tuples, one per row.
# Each tuple matches the SELECT column order:
#
#     (id: int, tier: str)
#
# Your function must:
#   - Accept a single argument `results` (list of tuples)
#   - Group customer IDs by their tier
#   - Return a list of dicts, one per tier, in this exact shape:
#
#       [
#           {"tier": "gold",   "customer_ids": [1, 2]},
#           {"tier": "silver", "customer_ids": [3]},
#           {"tier": "bronze", "customer_ids": [4, 5, 6]},
#       ]
#
# The order of tiers in the list does not matter. Each dict must have
# exactly the keys "tier" and "customer_ids".
#
# Hint: collections.defaultdict(list) is already imported above.
# ---------------------------------------------------------------------------

def group_ids_by_tier(results):
    ### YOUR CODE HERE
    pass


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

    # -----------------------------------------------------------------------
    # Task 1 — query active customer IDs from the database (already implemented)
    #
    # SQLExecuteQueryOperator runs the SQL and passes its results through
    # `output_processor`. The return value of output_processor becomes this
    # task's XCom output — a list of tier-grouped dicts ready for mapping.
    # -----------------------------------------------------------------------
    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        conn_id=CONN_ID,
        sql="""
            SELECT id, tier
            FROM customers
            WHERE is_active = TRUE
            ORDER BY tier, id
        """,
        # Wire your function in here once Exercise 1 is complete.
        # Replace `None` with the name of your output_processor function.
        output_processor=None,  ### YOUR CODE HERE
    )

    # -----------------------------------------------------------------------
    # EXERCISE 2 — write the dynamic task
    #
    # Create a @task called `send_outreach` that:
    #   - Accepts a single argument `group: dict` with keys "tier" and
    #     "customer_ids" (one dict from the list returned by group_ids_by_tier)
    #   - Iterates over every ID in group["customer_ids"]
    #   - Looks up the correct message template from OUTREACH_TEMPLATES
    #     using group["tier"] as the key, formatting {customer_id} into the
    #     string using .format(customer_id=customer_id)
    #   - Calls _send_message(customer_id, message) for each ID
    #   - Sets map_index_template so the UI shows the tier name
    #     instead of a numeric index (e.g. "send_outreach[gold]")
    #
    # Hint: use `{{ task.op_kwargs['group']['tier'] }}` as the template.
    # -----------------------------------------------------------------------

    ### YOUR CODE HERE

    # -----------------------------------------------------------------------
    # EXERCISE 3 — wire the dynamic expansion
    #
    # fetch_customers.output holds the list of tier dicts produced by
    # group_ids_by_tier. Expand send_outreach over that list so one task
    # instance is created per tier.
    #
    # Requirements:
    #   - Use .expand() to map over fetch_customers.output
    #   - Pass each element as the `group` kwarg to send_outreach
    #
    # The resulting dependency chain should be:
    #   fetch_customers >> send_outreach[gold]
    #                   >> send_outreach[silver]
    #                   >> send_outreach[bronze]
    # -----------------------------------------------------------------------

    ### YOUR CODE HERE