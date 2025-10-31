WITH order_week AS (
                SELECT
                    order_id,
                    customer_id,
                    DATE_TRUNC('week', order_date) AS order_week,
                    total_amount * (SELECT exchange_rate FROM exchange_rate WHERE currency = local_currency) AS total_amount_usd
                FROM {{ source('raw', 'raw_orders') }}
            )
            SELECT
                order_id,
                customer_id,
                TO_CHAR(order_week, 'IYYY-IW') AS order_week,
                total_amount_usd
            FROM order_week