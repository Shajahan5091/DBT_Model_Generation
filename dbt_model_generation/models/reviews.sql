WITH reviews_transformed AS (
                SELECT
                    review_id,
                    CASE
                        WHEN rating >= 4 THEN 'Positive'
                        WHEN rating = 3 THEN 'Neutral'
                        ELSE 'Negative'
                    END AS sentiment_category
                FROM {{ source('raw', 'raw_reviews') }}
            ),

            reviews AS (
                SELECT
                    review_id,
                    sentiment_category
                FROM reviews_transformed
                GROUP BY 1, 2
            )

            SELECT * FROM reviews;