{#
    Model: orders_enriched
    Description: Staging model that enriches raw orders data with customer information
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH raw_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM {{ source('raw', 'raw_orders') }}
),

raw_customers AS (
    SELECT 
        cust_id,
        full_name
    FROM {{ source('raw', 'raw_customers') }}
),

orders_with_customer AS (
    SELECT 
        ro.order_id::STRING AS order_id,
        ro.customer_id::STRING AS customer_id,
        ro.order_date::DATE AS order_date,
        ro.total_amount::FLOAT AS total_amount,
        rc.full_name::STRING AS customer_full_name
    FROM raw_orders ro
    LEFT JOIN raw_customers rc
        ON ro.customer_id = rc.cust_id
)

SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    customer_full_name
FROM orders_with_customer