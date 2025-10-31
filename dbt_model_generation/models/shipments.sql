WITH delivery_delay AS (
            SELECT
                shipment_id,
                DATEDIFF(delivery_date, order_date) AS delivery_delay_days
            FROM
                {{ source('raw', 'raw_shipments') }}
        )

        SELECT
            shipment_id,
            delivery_delay_days
        FROM
            delivery_delay