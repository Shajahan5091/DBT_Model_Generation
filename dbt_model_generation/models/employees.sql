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
        CAST(salary * 0.15 AS FLOAT) AS annual_bonus
        
    FROM source_data
    WHERE emp_id IS NOT NULL
      AND salary IS NOT NULL
)

SELECT * FROM transformed