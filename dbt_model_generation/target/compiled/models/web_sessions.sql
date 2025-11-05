

WITH source_data AS (
    SELECT 
        session_id,
        timestamp,
        url
    FROM demo_db.raw.raw_web_logs
),

transformed AS (
    SELECT
        -- Direct mapping for session_id
        session_id::STRING AS session_id,
        
        -- Extract hour from timestamp
        EXTRACT(HOUR FROM timestamp)::INTEGER AS session_hour,
        
        -- Extract domain from URL using string split
        SPLIT_PART(url, '/', 3)::STRING AS domain_name
        
    FROM source_data
    WHERE session_id IS NOT NULL
)

SELECT * FROM transformed