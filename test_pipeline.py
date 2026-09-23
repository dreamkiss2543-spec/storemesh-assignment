import sqlite3

with sqlite3.connect("analytics.db") as connection:
    customer_count = connection.execute(
        "SELECT COUNT(*) FROM dim_customers"
    ).fetchone()[0]

    order_count = connection.execute(
        "SELECT COUNT(*) FROM fct_orders"
    ).fetchone()[0]

    invalid_orders = connection.execute(
        "SELECT COUNT(*) FROM fct_orders WHERE total_amount <= 0"
    ).fetchone()[0]

assert customer_count == 10
assert order_count == 17
assert invalid_orders == 0

print("Tests passed: 10 customers, 17 valid orders")