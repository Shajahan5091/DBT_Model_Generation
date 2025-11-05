/*
  Model: stg_orders
  Description: Staging model for orders data with currency conversion and date transformations
  Created Date: 2024-12-19
  Author: Shajahan
*/

WITH source_data AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM {{ source('raw', 'raw_orders') }}
),

transformed AS (
    SELECT 
        -- Direct mapping for order_id
        CAST(order_id AS STRING) AS order_id,
        
        -- Direct mapping for customer_id
        CAST(customer_id AS STRING) AS customer_id,
        
        -- Derive ISO week from order_date
        TO_VARCHAR(DATE_PART('WEEK', order_date)) AS order_week,
        
        -- Convert from INR to USD (assuming conversion rate of 0.012)
        CAST(total_amount * 0.012 AS FLOAT) AS total_amount_usd
        
    FROM source_data
)

SELECT * FROM transformed