SELECT
                emp_id AS employee_id,
                salary * 0.15 AS annual_bonus
            FROM
                {{ source('raw', 'raw_employees') }}