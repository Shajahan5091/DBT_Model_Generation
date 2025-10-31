WITH discounted_products AS (
                SELECT
                    product_id,
                    CASE
                        WHEN category = 'Seasonal' THEN price * 0.9
                        ELSE price
                    END AS discounted_price
                FROM {{ source('raw', 'raw_products') }}
            ),

            products AS (
                SELECT
                    product_id,
                    discounted_price
                FROM discounted_products
            )

            SELECT * FROM products