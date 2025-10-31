SELECT
                product_id,
                CASE
                    WHEN category = 'Seasonal' THEN price * 0.9
                    ELSE price
                END AS discounted_price
            FROM {{ source('raw', 'raw_products') }}