WITH extracted_hour AS (
                SELECT
                    session_id,
                    EXTRACT(HOUR FROM timestamp) AS session_hour,
                    url
                FROM {{ source('raw', 'raw_web_logs') }}
            ),
            domain_extraction AS (
                SELECT
                    session_id,
                    session_hour,
                    SPLIT_PART(url, '/', 1) AS domain_name
                FROM extracted_hour
            )

            SELECT
                session_id,
                session_hour,
                domain_name
            FROM domain_extraction