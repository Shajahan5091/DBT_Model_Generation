With the provided schema mapping and source/target details, here are the DBT model SQL and YAML block:

        ```sql
        SELECT
            session_id,
            EXTRACT(HOUR FROM timestamp) AS session_hour,
            SPLIT_PART(url, '/', 3) AS domain_name
        FROM {{ source('raw', 'raw_web_logs') }}
        ```