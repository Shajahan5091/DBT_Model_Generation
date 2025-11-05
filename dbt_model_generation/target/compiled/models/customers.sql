

WITH source_data AS (
    SELECT 
        cust_id,
        first_name,
        last_name,
        signup_date,
        is_vip_flag
    FROM demo_db.raw.raw_customers
),

transformed AS (
    SELECT 
        -- Direct mapping with type casting
        CAST(cust_id AS STRING) AS customer_id,
        
        -- Concatenate first_name and last_name to create full_name
        CAST(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) AS STRING) AS full_name,
        
        -- Extract month name from signup_date
        CAST(MONTHNAME(signup_date) AS STRING) AS signup_month,
        
        -- Convert Y/N flag to boolean
        CASE 
            WHEN UPPER(is_vip_flag) = 'Y' THEN TRUE
            WHEN UPPER(is_vip_flag) = 'N' THEN FALSE
            ELSE NULL
        END AS is_vip
        
    FROM source_data
)

SELECT * FROM transformed