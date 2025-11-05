
    
    

with all_values as (

    select
        sentiment_category as value_field,
        count(*) as n_records

    from DEMO_DB.STAGING.reviews
    group by sentiment_category

)

select *
from all_values
where value_field not in (
    'Positive','Neutral','Negative'
)


