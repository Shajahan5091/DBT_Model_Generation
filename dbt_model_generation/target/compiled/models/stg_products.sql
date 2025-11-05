

WITH source_data AS (
    SELECT 
        sku,
        name,
        type,
        description,
        price
    FROM demo_db.raw.raw_products
),

transformed AS (
    SELECT 
        sku AS product_id,
        name AS product_name,
        type AS product_type,
        description AS product_description,
        -- Convert cents to dollars
        ROUND(price / 100.0, 2) AS product_price,
        -- Check if product is a food item
        CASE 
            WHEN LOWER(type) = 'jaffle' THEN TRUE 
            ELSE FALSE 
        END AS is_food_item,
        -- Check if product is a drink item
        CASE 
            WHEN LOWER(type) = 'beverages' THEN TRUE 
            ELSE FALSE 
        END AS is_drink_item
    FROM source_data
)

SELECT * FROM transformed