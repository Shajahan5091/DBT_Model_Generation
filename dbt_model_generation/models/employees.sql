/*
 * Model: stg_employees
 * Description: Staging model for employee data with calculated annual bonus
 * Created Date: 2024-01-15
 * Author: Shajahan
 */

WITH source_data AS (
    SELECT 
        emp_id,
        salary
    FROM {{ source('raw', 'raw_employees') }}
),

transformed AS (
    SELECT 
        -- Direct mapping with type casting
        CAST(emp_id AS STRING) AS employee_id,
        
        -- Calculate 15% of salary as annual bonus
        CAST(salary * 0.15 AS FLOAT) AS annual_bonus,
        
        -- Keep original salary for reference
        salary
        
    FROM source_data
)

SELECT 
    employee_id,
    annual_bonus
FROM transformed