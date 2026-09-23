import sqlite3
from prefect import flow, task
@task
def read_customers():
    connection = sqlite3.connect("shopdata.db")
    customers = connection.execute("""
        SELECT customer_id, full_name, email, phone, signup_date
        FROM vw_raw_customers
        ORDER BY signup_date DESC
    """).fetchall()
    connection.close()
    return customers
@task
def read_orders():
    connection = sqlite3.connect("shopdata.db")
    orders = connection.execute("""
        SELECT order_id, customer_id, order_date, total_amount, currency, status
        FROM vw_raw_orders
        WHERE total_amount > 0
    """).fetchall()
    connection.close()
    return orders
@task
def read_rates():
    connection = sqlite3.connect("shopdata.db")
    rates = connection.execute("""
        SELECT currency, date, rate_to_usd
        FROM vw_exchange_rates
    """).fetchall()
    connection.close()
    return rates
@task
def deduplicate_customers(customers):
    latest_customers = {}
    for customer in customers:
        customer_id = customer[0]
        if customer_id not in latest_customers:
            latest_customers[customer_id] = customer
    return latest_customers
@task
def clean_customers(latest_customers):
    cleaned_customers = []
    missing_email_count = 0
    phones_fixed = 0

    for customer in latest_customers.values():
        customer_id, full_name, email, phone, signup_date = customer

        if email is None or email.strip() == "":
            email = "unknown@domain.com"
            missing_email_count += 1

        if phone is not None:
            original_phone = phone
            phone = "".join(char for char in phone if char.isdigit())
            if phone != original_phone:
                phones_fixed += 1

        cleaned_customers.append(
            (customer_id, full_name, email, phone, signup_date)
        )

    return cleaned_customers, missing_email_count, phones_fixed
@task
def convert_orders(orders, rates):
    exchange_rates = {}
    for currency, rate_date, rate_to_usd in rates:
        exchange_rates[(currency, rate_date)] = rate_to_usd

    converted_orders = []
    fallback_count = 0

    for order in orders:
        order_id, customer_id, order_date, total_amount, currency, status = order

        rate = exchange_rates.get((currency, order_date))
        if currency == "USD":
            rate = 1.0
        elif rate is None:
            rate = 1.0
            fallback_count += 1

        amount_usd = round(total_amount * rate, 2)
        converted_orders.append(
            (order_id, customer_id, order_date,
             total_amount, currency, status, amount_usd)
        )

    return converted_orders, fallback_count, len(exchange_rates)

@flow(name="shopdata-etl", log_prints=True)
def run_pipeline():
    customers = read_customers()

    latest_customers = deduplicate_customers(customers)

    print(f"Raw customers: {len(customers)}")
    print(f"Unique customers: {len(latest_customers)}")

    cleaned_customers, missing_email_count, phones_fixed = clean_customers(latest_customers)

    print(f"Missing emails filled: {missing_email_count}")
    print(f"Phones cleaned: {phones_fixed}")

    output = sqlite3.connect("analytics.db")

    output.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            signup_date TEXT
        )
    """)

    output.execute("DELETE FROM dim_customers")

    output.executemany("""
        INSERT INTO dim_customers
        (customer_id, full_name, email, phone, signup_date)
        VALUES (?, ?, ?, ?, ?)
    """, cleaned_customers)

    output.commit()
    output.close()

    print(f"Saved customers: {len(cleaned_customers)}")

    orders = read_orders()
    print(f"Valid orders: {len(orders)}")

    rates = read_rates()

    converted_orders, fallback_count, rate_count = convert_orders(orders, rates)
    print(f"Exchange rates loaded: {rate_count}")

    print(f"Orders converted: {len(converted_orders)}")
    print(f"Orders without matching rate: {fallback_count}")

    output = sqlite3.connect("analytics.db")

    output.execute("""
        CREATE TABLE IF NOT EXISTS fct_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            total_amount REAL,
            currency TEXT,
            status TEXT,
            usd_amount REAL
        )
    """)

    output.execute("DELETE FROM fct_orders")

    output.executemany("""
        INSERT INTO fct_orders
        (order_id, customer_id, order_date, total_amount,
        currency, status, usd_amount)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, converted_orders)

    output.commit()
    output.close()

    print(f"Saved orders: {len(converted_orders)}")

if __name__ == "__main__":
    run_pipeline()