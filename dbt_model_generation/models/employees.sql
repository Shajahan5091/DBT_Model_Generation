WITH employee_data AS (
                SELECT
                    emp_id AS employee_id,
                    salary * 0.15 AS annual_bonus
                FROM {{ source('raw', 'raw_employees') }}
            ),

            employee_tests AS (
                SELECT
                    COUNT(DISTINCT employee_id) AS unique_employee_count,
                    COUNT(CASE WHEN annual_bonus IS NOT NULL THEN 1 END) AS not_null_annual_bonus_count
                FROM employee_data
            )

            SELECT * FROM employee_data
            UNION ALL
            SELECT * FROM employee_tests;