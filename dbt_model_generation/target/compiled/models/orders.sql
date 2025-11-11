

WITH source_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM demo_db.raw.raw_orders
),

transformed_orders AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,
        -- Convert order_date to ISO week format (YYYY-WW)
        TO_VARCHAR(DATE_PART('year', order_date)) || '-' || 
        LPAD(TO_VARCHAR(DATE_PART('week', order_date)), 2, '0') AS order_week,
        -- Convert INR to USD (assuming conversion rate, this should be parameterized)
        CAST(total_amount * 0.012 AS FLOAT) AS total_amount_usd
    FROM source_orders
    WHERE order_id IS NOT NULL
)

SELECT * FROM transformed_orders