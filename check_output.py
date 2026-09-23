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
    usd_amounts = dict(connection.execute("""
        SELECT order_id, usd_amount
        FROM fct_orders
        WHERE order_id IN (102, 105, 107, 115)
    """).fetchall())

assert customer_count == 10
assert order_count == 17
assert invalid_orders == 0
assert usd_amounts == {
    102: 220.0,    # EUR มีอัตราแลกเปลี่ยน
    105: 69.0,     # JPY มีอัตราแลกเปลี่ยน
    107: 120.0,    # ไม่มีสกุลเงิน
    115: 25000.0,  # ไม่พบอัตราแลกเปลี่ยน
}
print("Tests passed: 10 customers, 17 valid orders")