

WITH source_data AS (
    SELECT
        id,
        name
    FROM demo_db.raw.raw_customers
),

renamed AS (
    SELECT
        CAST(id AS TEXT) AS customer_id,
        CAST(name AS TEXT) AS customer_name
    FROM source_data
)

SELECT * FROM renamed