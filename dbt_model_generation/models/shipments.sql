{#
    Model: stg_shipments
    Description: Staging model for shipments data with delivery delay calculation
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM {{ source('raw', 'raw_shipments') }}
),

transformed AS (
    SELECT 
        -- Direct mapping with proper casting
        CAST(shipment_id AS STRING) AS shipment_id,
        
        -- Calculate delivery delay days
        CAST(DATEDIFF('day', order_date, delivery_date) AS INTEGER) AS delivery_delay_days,
        
        -- Keep original dates for reference
        delivery_date,
        order_date
        
    FROM source_data
)

SELECT * FROM transformed