

WITH source_data AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM demo_db.raw.raw_orders
),

transformed AS (
    SELECT 
        -- Direct mapping for order_id
        CAST(order_id AS STRING) AS order_id,
        
        -- Direct mapping for customer_id
        CAST(customer_id AS STRING) AS customer_id,
        
        -- Derive ISO week from order_date
        TO_VARCHAR(YEAR(order_date)) || '-W' || 
        LPAD(TO_VARCHAR(WEEK(order_date)), 2, '0') AS order_week,
        
        -- Convert from INR to USD (assuming conversion rate of 0.012)
        CAST(total_amount * 0.012 AS FLOAT) AS total_amount_usd
        
    FROM source_data
)

SELECT * FROM transformed