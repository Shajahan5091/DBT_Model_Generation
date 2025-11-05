{#
    Model: stg_reviews
    Description: Staging model for reviews data with sentiment categorization based on rating
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_data AS (
    SELECT 
        review_id,
        rating
    FROM {{ source('raw', 'raw_reviews') }}
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