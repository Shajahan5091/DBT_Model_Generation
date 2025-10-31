SELECT
                review_id,
                CASE
                    WHEN rating >= 4 THEN 'Positive'
                    WHEN rating = 3 THEN 'Neutral'
                    ELSE 'Negative'
                END AS sentiment_category
            FROM
                {{ source('raw', 'raw_reviews') }}