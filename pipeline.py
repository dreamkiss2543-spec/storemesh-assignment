import sqlite3
from prefect import flow, task
from prefect.logging import get_run_logger
def standardize_phone(phone):
    if phone is None:
        return None
    return "".join(char for char in phone if char.isdigit())
def convert_amount_to_usd(total_amount, currency, order_date, exchange_rates):
    if currency == "USD":
        return round(total_amount, 2), False

    rate = exchange_rates.get((currency, order_date))
    if rate is None:
        return round(total_amount, 2), True

    return round(total_amount * rate, 2), False
@task
def read_customers():
    logger = get_run_logger()
    connection = None

    try:
        connection = sqlite3.connect("shopdata.db")
        customers = connection.execute("""
            SELECT customer_id, full_name, email, phone, signup_date
            FROM vw_raw_customers
            ORDER BY signup_date DESC
        """).fetchall()
        logger.info("Read %s customer rows", len(customers))
        return customers

    except sqlite3.Error:
        logger.exception("Could not read customers from shopdata.db")
        raise

    finally:
        if connection is not None:
            connection.close()
@task
def read_orders():
    logger = get_run_logger()
    connection = None

    try:
        connection = sqlite3.connect("shopdata.db")
        orders = connection.execute("""
            SELECT order_id, customer_id, order_date, total_amount, currency, status
            FROM vw_raw_orders
            WHERE total_amount > 0
        """).fetchall()
        logger.info("Read %s valid orders", len(orders))
        return orders

    except sqlite3.Error:
        logger.exception("Could not read orders from shopdata.db")
        raise

    finally:
        if connection is not None:
            connection.close()
@task
def read_rates():
    logger = get_run_logger()
    connection = None

    try:
        connection = sqlite3.connect("shopdata.db")
        rates = connection.execute("""
            SELECT currency, date, rate_to_usd
            FROM vw_exchange_rates
        """).fetchall()
        logger.info("Read %s exchange rates", len(rates))
        return rates

    except sqlite3.Error:
        logger.exception("Could not read exchange rates from shopdata.db")
        raise

    finally:
        if connection is not None:
            connection.close()
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
            phone = standardize_phone(phone)
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

        amount_usd, used_fallback = convert_amount_to_usd(
            total_amount, currency, order_date, exchange_rates
        )
        if used_fallback:
            fallback_count += 1

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