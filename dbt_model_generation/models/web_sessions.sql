{#
    Model: web_sessions
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
        session_id::STRING AS session_id,
        EXTRACT(HOUR FROM timestamp)::INTEGER AS session_hour,
        SPLIT_PART(url, '/', 3)::STRING AS domain_name
    FROM source_data
)

SELECT 
    session_id,
    session_hour,
    domain_name
FROM transformed