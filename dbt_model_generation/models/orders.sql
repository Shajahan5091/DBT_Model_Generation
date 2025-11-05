{#
    Model: orders
    Description: Staging model for orders data with currency conversion and date transformations
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM {{ source('raw', 'raw_orders') }}
),

transformed_orders AS (
    SELECT 
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,
        TO_VARCHAR(DATE_TRUNC('week', order_date), 'YYYY-"W"WW') AS order_week,
        CAST(total_amount / 83.0 AS FLOAT) AS total_amount_usd  -- Converting INR to USD assuming exchange rate of 83
    FROM source_orders
)

SELECT * FROM transformed_orders