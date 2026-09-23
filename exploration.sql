SELECT customer_id, COUNT(*) AS row_count
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check missing customer emails
SELECT COUNT(*) AS missing_emails
FROM vw_raw_customers
WHERE email IS NULL OR TRIM(email) = '';

-- Check nonpositive order amounts
SELECT order_id, total_amount
FROM vw_raw_orders
WHERE total_amount <= 0;

-- Orders with missing currency
SELECT order_id, currency
FROM vw_raw_orders
WHERE currency IS NULL OR currency = '';