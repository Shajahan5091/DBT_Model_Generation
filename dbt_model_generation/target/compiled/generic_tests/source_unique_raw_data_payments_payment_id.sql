
    
    

select
    payment_id as unique_field,
    count(*) as n_records

from raw_db.raw_data.payments
where payment_id is not null
group by payment_id
having count(*) > 1


