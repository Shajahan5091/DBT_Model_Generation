/*
 * Model Name: stg_employees
 * Description: Staging model for employees data with transformations for employee_id and annual_bonus calculation
 * Created Date: 2024-12-19
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
        -- Direct mapping of emp_id to employee_id
        CAST(emp_id AS STRING) AS employee_id,
        
        -- Calculate 15% of salary as annual bonus
        CAST(salary * 0.15 AS FLOAT) AS annual_bonus
        
    FROM source_data
)

SELECT * FROM transformed