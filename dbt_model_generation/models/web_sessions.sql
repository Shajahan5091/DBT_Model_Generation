WITH source_data AS (
    SELECT 
        session_id,
        timestamp,
        url
    FROM {{ source('raw', 'raw_web_logs') }}
),

transformed AS (
    SELECT 
        -- Direct mapping for session_id
        session_id::STRING AS session_id,
        
        -- Extract hour from timestamp
        EXTRACT(HOUR FROM timestamp)::INTEGER AS session_hour,
        
        -- Extract domain from url using string split
        SPLIT_PART(url, '/', 3)::STRING AS domain_name
        
    FROM source_data
)

SELECT * FROM transformed