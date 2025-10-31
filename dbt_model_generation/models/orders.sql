WITH raw_data AS (
                SELECT
                    order_id,
                    customer_id,
                    order_date,
                    total_amount
                FROM
                    {{ source('raw', 'raw_orders') }}
            ),
            exchange_rate AS (
                -- Assuming you have a separate model for exchange_rate
                SELECT
                    rate AS exchange_rate
                FROM
                    {{ ref('exchange_rate') }}
            ),
            transformed_data AS (
                SELECT
                    rd.order_id,
                    rd.customer_id,
                    TO_CHAR(rd.order_date, 'IYYY-IW') AS order_week,
                    rd.total_amount * er.exchange_rate AS total_amount_usd
                FROM
                    raw_data rd
                CROSS JOIN
                    exchange_rate er
            )
            SELECT
                order_id,
                customer_id,
                order_week,
                total_amount_usd
            FROM
                transformed_data