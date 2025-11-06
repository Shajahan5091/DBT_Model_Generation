{#
    Model: customers
    Description: Staging model for customer data with transformations for customer ID, full name, signup month, and VIP status
    Created Date: 2024-12-19
    Author: AI Generated
#}

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
        -- Direct mapping for customer ID
        cust_id::STRING AS customer_id,
        
        -- Concatenate first_name and last_name for full_name
        CONCAT(
            COALESCE(first_name, ''), 
            CASE 
                WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN ' '
                ELSE ''
            END,
            COALESCE(last_name, '')
        )::STRING AS full_name,
        
        -- Extract month name from signup_date
        MONTHNAME(signup_date)::STRING AS signup_month,
        
        -- Convert Y/N to TRUE/FALSE for VIP status
        CASE 
            WHEN UPPER(is_vip_flag) = 'Y' THEN TRUE
            WHEN UPPER(is_vip_flag) = 'N' THEN FALSE
            ELSE NULL
        END AS is_vip
        
    FROM source_data
)

SELECT * FROM transformed