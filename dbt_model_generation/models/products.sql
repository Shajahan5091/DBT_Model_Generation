WITH source AS (
                SELECT
                    product_id,
                    price,
                    category
                FROM {{ source('raw', 'raw_products') }}
            ),
            transform AS (
                SELECT
                    product_id,
                    CASE
                        WHEN category = 'Seasonal' THEN price * 0.9
                        ELSE price
                    END AS discounted_price
                FROM source
            )
            SELECT
                product_id,
                discounted_price
            FROM transform