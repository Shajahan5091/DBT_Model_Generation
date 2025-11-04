WITH source_data AS (
    SELECT 
        product_id,
        price,
        category
    FROM {{ source('raw', 'raw_products') }}
),

transformed AS (
    SELECT 
        -- Direct mapping for product_id
        CAST(product_id AS STRING) AS product_id,
        
        -- Apply 10% discount if category = 'Seasonal'
        CAST(
            CASE 
                WHEN UPPER(TRIM(category)) = 'SEASONAL' 
                THEN price * 0.9 
                ELSE price 
            END AS FLOAT
        ) AS discounted_price
        
    FROM source_data
    WHERE product_id IS NOT NULL
)

SELECT * FROM transformed