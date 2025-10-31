WITH cte_raw_data AS (
            SELECT
                cust_id,
                first_name || ' ' || last_name AS full_name,
                TO_CHAR(signup_date, 'Month') AS signup_month,
                CASE
                    WHEN is_vip_flag = 'Y' THEN TRUE
                    ELSE FALSE
                END AS is_vip
            FROM {{ source('raw', 'raw_customers') }}
        ),

        SELECT
            cust_id AS customer_id,
            full_name,
            signup_month,
            is_vip
        FROM cte_raw_data