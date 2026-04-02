# Exercise: Dynamic Task Mapping

## Scenario

- The **connection_id** `customer_subscriptions` points to a database with a `customers` table.
- In this table, a  `tier` column is used to track each customer's subscription level. (`gold`,
`silver`, `bronze`). 
- Each week, personalised outreach messages need to be
sent to all active customers, processed in parallel by tier.

The pipeline has three steps:

```
fetch_customers >> group_by_tier >> send_outreach[gold]
                                 >> send_outreach[silver]
                                 >> send_outreach[bronze]
```

- `fetch_customers` queries the database and passes raw rows downstream.
- `group_by_tier` restructures those rows into one dict per tier.
- `send_outreach` is dynamically mapped — Airflow creates one instance per tier dict so each tier is processed in parallel.

---

## Your tasks

1. Trigger the `init_database` DAG to seed the databaase with records.
1. In the **`group_by_tier`** task, transform the `defaultdict` so downstream tasks can be dynamically generated for each tier
1. For the **`send_outreach`** task, set the `map_index_template` so each mapped instance
   is labelled by tier name in the UI.
1. Set the task dependencies so `send_outreach` is mapped to each tier.