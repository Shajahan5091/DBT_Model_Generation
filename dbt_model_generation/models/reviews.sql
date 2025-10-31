WITH transformed_reviews AS (
                SELECT
                    review_id,
                    CASE
                        WHEN rating >= 4 THEN 'Positive'
                        WHEN rating = 3 THEN 'Neutral'
                        ELSE 'Negative'
                    END AS sentiment_category
                FROM {{ source('raw', 'raw_reviews') }}
            )

            SELECT
                review_id,
                sentiment_category
            FROM transformed_reviews