

WITH source_data AS (
    SELECT 
        emp_id,
        salary
    FROM demo_db.raw.raw_employees
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