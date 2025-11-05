{#
    Model: reviews
    Description: Staging model for reviews data with sentiment categorization based on rating
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_reviews AS (
    SELECT 
        review_id,
        rating
    FROM {{ source('raw', 'raw_reviews') }}
),

transformed_reviews AS (
    SELECT 
        CAST(review_id AS STRING) AS review_id,
        CASE 
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating = 3 THEN 'Neutral'
            ELSE 'Negative'
        END AS sentiment_category
    FROM source_reviews
)

SELECT * FROM transformed_reviews