/*
 * Model Name: stg_shipments
 * Description: Staging model for shipments data with delivery delay calculation
 * Created Date: 2024-12-19
 * Author: Shajahan
 */

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
        DATEDIFF('day', order_date, delivery_date) AS delivery_delay_days
    FROM source_data
)

SELECT * FROM transformed