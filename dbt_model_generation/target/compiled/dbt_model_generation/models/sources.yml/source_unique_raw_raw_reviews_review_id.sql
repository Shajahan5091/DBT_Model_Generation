
    
    

select
    review_id as unique_field,
    count(*) as n_records

from demo_db.raw.raw_reviews
where review_id is not null
group by review_id
having count(*) > 1


