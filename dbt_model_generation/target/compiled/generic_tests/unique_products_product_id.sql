
    
    

select
    product_id as unique_field,
    count(*) as n_records

from DEMO_DB.STAGING.products
where product_id is not null
group by product_id
having count(*) > 1


