WITH source AS (
                SELECT
                    product_id,
                    quantity
                FROM {{ source('raw', 'raw_inventory') }}
            ),

            transformed AS (
                SELECT
                    product_id,
                    CASE
                        WHEN quantity > 0 THEN 'IN_STOCK'
                        ELSE 'OUT_OF_STOCK'
                    END AS stock_status
                FROM source
            )

            SELECT
                product_id,
                stock_status
            FROM transformed