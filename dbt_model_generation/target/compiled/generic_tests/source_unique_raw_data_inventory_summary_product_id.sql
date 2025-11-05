
    
    

select
    product_id as unique_field,
    count(*) as n_records

from raw_db.raw_data.inventory_summary
where product_id is not null
group by product_id
having count(*) > 1


