{#
    Model: stg_products
    Description: Staging model for products data with discount logic applied
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_data AS (
    SELECT 
        product_id,
        price,
        category
    FROM {{ source('raw', 'raw_products') }}
),

transformed AS (
    SELECT 
        CAST(product_id AS STRING) AS product_id,
        CASE 
            WHEN UPPER(category) = 'SEASONAL' 
            THEN CAST(price * 0.9 AS FLOAT)
            ELSE CAST(price AS FLOAT)
        END AS discounted_price
    FROM source_data
    WHERE product_id IS NOT NULL
)

SELECT * FROM transformed