

WITH source_data AS (
    SELECT 
        product_id,
        quantity
    FROM demo_db.raw.raw_inventory
),

transformed AS (
    SELECT 
        CAST(product_id AS STRING) AS product_id,
        CASE 
            WHEN quantity > 0 THEN 'IN_STOCK'
            ELSE 'OUT_OF_STOCK'
        END AS stock_status
    FROM source_data
)

SELECT * FROM transformed