WITH transformed_data AS (
    SELECT
        cust_id AS customer_id,
        CONCAT(first_name, ' ', last_name) AS full_name,
        TO_CHAR(signup_date, 'Month') AS signup_month,
        CASE
            WHEN is_vip_flag = 'Y' THEN TRUE
            ELSE FALSE
        END AS is_vip
    FROM {{ source('raw', 'raw_customers') }}
),

tests AS (
    SELECT
        customer_id,
        full_name,
        signup_month,
        is_vip,
        COUNT(*) OVER (PARTITION BY customer_id) AS cnt_customer_id,
        CASE
            WHEN full_name IS NULL THEN 'fail'
            ELSE 'pass'
        END AS test_full_name,
        CASE
            WHEN signup_month IS NULL THEN 'fail'
            ELSE 'pass'
        END AS test_signup_month,
        CASE
            WHEN is_vip NOT IN ('true', 'false') THEN 'fail'
            ELSE 'pass'
        END AS test_is_vip
    FROM transformed_data
)

SELECT
    customer_id,
    full_name,
    signup_month,
    is_vip
FROM tests
WHERE cnt_customer_id = 1
AND test_full_name = 'pass'
AND test_signup_month = 'pass'
AND test_is_vip = 'pass'