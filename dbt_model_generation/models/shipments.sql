WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM {{ source('raw', 'raw_shipments') }}
),

transformed AS (
    SELECT 
        -- Direct mapping for shipment_id
        CAST(shipment_id AS STRING) AS shipment_id,
        
        -- Calculate delivery delay days as difference between delivery_date and order_date
        CAST(DATEDIFF('day', order_date, delivery_date) AS INTEGER) AS delivery_delay_days
        
    FROM source_data
    WHERE shipment_id IS NOT NULL
)

SELECT * FROM transformed