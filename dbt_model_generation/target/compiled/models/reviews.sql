

WITH source_reviews AS (
    SELECT 
        review_id,
        rating
    FROM demo_db.raw.raw_reviews
),

transformed_reviews AS (
    SELECT 
        review_id::STRING AS review_id,
        CASE 
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating = 3 THEN 'Neutral'
            ELSE 'Negative'
        END AS sentiment_category
    FROM source_reviews
    WHERE review_id IS NOT NULL
)

SELECT * FROM transformed_reviews