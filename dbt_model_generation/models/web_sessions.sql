{#
    Model: stg_web_sessions
    Description: Staging model for web sessions data with extracted domain and session hour
    Created Date: 2024-12-19
    Author: AI Generated
#}

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
        EXTRACT(HOUR FROM timestamp) AS session_hour,
        
        -- Extract domain from URL using string split
        SPLIT_PART(url, '/', 3) AS domain_name
        
    FROM source_data
    WHERE session_id IS NOT NULL
)

SELECT 
    session_id,
    session_hour,
    domain_name
FROM transformed