WITH source_data AS (
    SELECT 
        cust_id,
        first_name,
        last_name,
        signup_date,
        is_vip_flag
    FROM {{ source('raw', 'raw_customers') }}
),

transformed AS (
    SELECT
        -- Direct mapping for customer_id
        cust_id::STRING AS customer_id,
        
        -- Concatenate first_name and last_name for full_name
        CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))::STRING AS full_name,
        
        -- Extract month name from signup_date
        MONTHNAME(signup_date)::STRING AS signup_month,
        
        -- Convert Y/N flag to TRUE/FALSE boolean
        CASE 
            WHEN UPPER(is_vip_flag) = 'Y' THEN TRUE
            WHEN UPPER(is_vip_flag) = 'N' THEN FALSE
            ELSE NULL
        END AS is_vip
        
    FROM source_data
)

SELECT * FROM transformed