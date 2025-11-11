
    
    

select
    cust_id as unique_field,
    count(*) as n_records

from demo_db.raw.raw_customers
where cust_id is not null
group by cust_id
having count(*) > 1


