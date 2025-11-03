WITH source_data AS (
    SELECT 
        review_id,
        rating
    FROM {{ source('raw', 'raw_reviews') }}
),

transformed AS (
    SELECT 
        CAST(review_id AS STRING) AS review_id,
        CASE 
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating = 3 THEN 'Neutral'
            ELSE 'Negative'
        END AS sentiment_category
    FROM source_data
)

SELECT * FROM transformed