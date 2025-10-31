WITH delivery_delay AS (
                SELECT
                    shipment_id,
                    DATEDIFF(day, order_date, delivery_date) AS delivery_delay_days
                FROM {{ source('raw', 'raw_shipments') }}
            ),

            shipments AS (
                SELECT
                    shipment_id,
                    delivery_delay_days
                FROM delivery_delay
            )

            SELECT * FROM shipments;