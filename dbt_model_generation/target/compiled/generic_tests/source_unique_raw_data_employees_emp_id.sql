
    
    

select
    emp_id as unique_field,
    count(*) as n_records

from raw_db.raw_data.employees
where emp_id is not null
group by emp_id
having count(*) > 1


