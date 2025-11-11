
    
    

select
    emp_id as unique_field,
    count(*) as n_records

from demo_db.raw.raw_employees
where emp_id is not null
group by emp_id
having count(*) > 1


