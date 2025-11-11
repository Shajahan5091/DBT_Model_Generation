

WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM demo_db.raw.raw_shipments
),

transformed AS (
    SELECT
        CAST(shipment_id AS STRING) AS shipment_id,
        CAST(DATEDIFF('day', order_date, delivery_date) AS INTEGER) AS delivery_delay_days
    FROM source_data
)

SELECT * FROM transformed