WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM {{ source('raw', 'raw_shipments') }}
),

transformed AS (
    SELECT 
        CAST(shipment_id AS STRING) AS shipment_id,
        CAST(DATEDIFF('day', order_date, delivery_date) AS INTEGER) AS delivery_delay_days
    FROM source_data
    WHERE shipment_id IS NOT NULL
      AND delivery_date IS NOT NULL
      AND order_date IS NOT NULL
)

SELECT * FROM transformed