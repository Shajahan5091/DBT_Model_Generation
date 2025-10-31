WITH cte AS (
                SELECT
                    product_id,
                    CASE
                        WHEN quantity > 0 THEN 'IN_STOCK'
                        ELSE 'OUT_OF_STOCK'
                    END AS stock_status
                FROM {{ source('raw', 'raw_inventory') }}
            )
            SELECT * FROM cte