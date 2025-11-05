

WITH source_data AS (
    SELECT 
        review_id,
        rating
    FROM demo_db.raw.raw_reviews
),

transformed AS (
    SELECT 
        -- Direct mapping for review_id
        CAST(review_id AS STRING) AS review_id,
        
        -- Transform rating to sentiment_category based on business logic
        CASE 
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating = 3 THEN 'Neutral'
            ELSE 'Negative'
        END AS sentiment_category
        
    FROM source_data
)

SELECT * FROM transformed