{#
    Model: stg_employees
    Description: Staging model for employees data with transformed salary to annual bonus calculation
    Created Date: 2024-12-19
    Author: AI Generated
#}

WITH source_data AS (
    SELECT 
        emp_id,
        salary
    FROM {{ source('raw', 'raw_employees') }}
),

transformed AS (
    SELECT 
        CAST(emp_id AS STRING) AS employee_id,
        CAST(salary * 0.15 AS FLOAT) AS annual_bonus
    FROM source_data
)

SELECT * FROM transformed