

from __future__ import annotations

from datetime import datetime
from collections import defaultdict

from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ---------------------------------------------------------------------------
# Constants (do not modify)
# ---------------------------------------------------------------------------

CONN_ID = "crm_postgres"
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
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="customer_outreach_pipeline",
    tags=["exercise", "dynamic-mapping"],
):

    # -----------------------------------------------------------------------
    # Task 1 — query active customer IDs from the database (already implemented)
    #
    # output_processor=lambda results: results passes the raw rows through
    # unchanged into XCom as a list of (id, tier) tuples. Grouping is handled
    # in the @task below so that the full result set is received at once.
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
        output_processor=lambda results: results,
    )

    # -----------------------------------------------------------------------
    # Task 2 — group raw rows by tier (already implemented except return)
    #
    # Receives the full list of (id, tier) tuples from fetch_customers and
    # groups them into a list of dicts, one per tier. Airflow can then fan
    # .expand() out over this list to create one task instance per tier.
    #
    # EXERCISE 1 — complete the return statement.
    # Build and return a list of dicts in this shape:
    #   [
    #       {"tier": "gold",   "customer_ids": [1, 2]},
    #       {"tier": "silver", "customer_ids": [3]},
    #       {"tier": "bronze", "customer_ids": [4, 5, 6]},
    #   ]
    # The `grouped` defaultdict is already populated for you above the stub.
    # -----------------------------------------------------------------------
    @task
    def group_by_tier(results: list) -> list[dict]:
        grouped = defaultdict(list)
        for customer_id, tier in results:
            grouped[tier].append(customer_id)

        # Return a list of dicts, one per tier, using `grouped`.
        ### YOUR CODE HERE

    # -----------------------------------------------------------------------
    # Task 3 — send outreach messages for one tier (already implemented except
    # map_index_template)
    #
    # One instance of this task is created per tier dict in the list returned
    # by group_by_tier. Each instance receives a single group dict with keys
    # "tier" and "customer_ids".
    #
    # EXERCISE 2 — set map_index_template on the @task decorator so the
    # Airflow UI labels each instance with its tier name rather than a
    # numeric index (e.g. "send_outreach[gold]" not "send_outreach[0]").
    # Access the tier via: {{ task.op_kwargs['group']['tier'] }}
    # -----------------------------------------------------------------------
    @task(map_index_template=None)  ### YOUR CODE HERE
    def send_outreach(group: dict) -> None:
        tier         = group["tier"]
        customer_ids = group["customer_ids"]
        template     = OUTREACH_TEMPLATES[tier]

        print(f"Processing tier: {tier} ({len(customer_ids)} customers)")
        for customer_id in customer_ids:
            message = template.format(customer_id=customer_id)
            _send_message(customer_id, message)

    # -----------------------------------------------------------------------
    # EXERCISE 3 — set the task dependencies.
    #
    # 1. Call group_by_tier, passing fetch_customers.output as its argument,
    #    and assign the result to a variable called `groups`.
    # 2. Call send_outreach.expand(), passing `groups` as the `group` kwarg,
    #    so one task instance is created per tier dict.
    #
    # The resulting dependency chain should be:
    #   fetch_customers >> group_by_tier >> send_outreach[gold]
    #                                    >> send_outreach[silver]
    #                                    >> send_outreach[bronze]
    # -----------------------------------------------------------------------

    ### YOUR CODE HERE