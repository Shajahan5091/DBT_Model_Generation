

WITH source_data AS (
    SELECT 
        emp_id,
        salary
    FROM demo_db.raw.raw_employees
),

transformed AS (
    SELECT
        CAST(emp_id AS STRING) AS employee_id,
        CAST(salary * 0.15 AS FLOAT) AS annual_bonus
    FROM source_data
)

SELECT * FROM transformed