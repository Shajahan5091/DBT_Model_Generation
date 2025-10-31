WITH raw_data AS (
            SELECT
                session_id,
                EXTRACT(HOUR FROM timestamp) AS session_hour,
                SPLIT_PART(url, '/', 3) AS domain_name
            FROM {{ source('raw', 'raw_web_logs') }}
        )

        SELECT
            session_id,
            session_hour,
            domain_name
        FROM raw_data