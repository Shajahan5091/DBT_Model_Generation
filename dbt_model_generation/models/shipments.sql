WITH source_data AS (
    SELECT 
        shipment_id,
        delivery_date,
        order_date
    FROM {{ source('raw', 'raw_shipments') }}
),

transformed AS (
    SELECT 
        shipment_id::STRING AS shipment_id,
        DATEDIFF('day', order_date, delivery_date)::INTEGER AS delivery_delay_days
    FROM source_data
)

SELECT * FROM transformed