WITH cte AS (
                SELECT
                    cust_id AS customer_id,
                    CONCAT(first_name, ' ', last_name) AS full_name,
                    TO_CHAR(signup_date, 'Month') AS signup_month,
                    CASE WHEN is_vip_flag = 'Y' THEN TRUE ELSE FALSE END AS is_vip
                FROM {{ source('raw', 'raw_customers') }}
            )
            SELECT * FROM cte