WITH raw_orders AS (
            SELECT * FROM {{ source('raw', 'raw_orders') }}
        ),
        order_week AS (
            SELECT
                order_id,
                customer_id,
                DATEPART(ISO_WEEK, order_date) AS order_week,
                total_amount * (SELECT exchange_rate FROM exchange_rate WHERE currency = local_currency) AS total_amount_usd
            FROM raw_orders
        )
        SELECT
            order_id,
            customer_id,
            order_week,
            total_amount_usd
        FROM order_week