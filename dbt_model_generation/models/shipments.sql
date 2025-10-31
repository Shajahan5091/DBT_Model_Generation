WITH delivery_delays AS (
                SELECT
                    shipment_id,
                    DATEDIFF(delivery_date, order_date) AS delivery_delay_days
                FROM {{ source('raw', 'raw_shipments') }}
            )

            SELECT
                s.shipment_id,
                d.delivery_delay_days
            FROM {{ source('raw', 'raw_shipments') }} s
            JOIN delivery_delays d ON s.shipment_id = d.shipment_id