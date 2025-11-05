

WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM demo_db.raw.raw_shipments
),

transformed AS (
    SELECT 
        -- Direct mapping
        CAST(shipment_id AS STRING) AS shipment_id,
        
        -- Calculate delivery delay in days
        CAST(DATEDIFF('day', order_date, delivery_date) AS INTEGER) AS delivery_delay_days,
        
        -- Keep original dates for reference
        delivery_date,
        order_date
        
    FROM source_data
)

SELECT * FROM transformed