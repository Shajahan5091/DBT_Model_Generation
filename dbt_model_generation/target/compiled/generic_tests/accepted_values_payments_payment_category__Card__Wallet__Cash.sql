
    
    

with all_values as (

    select
        payment_category as value_field,
        count(*) as n_records

    from DEMO_DB.STAGING.payments
    group by payment_category

)

select *
from all_values
where value_field not in (
    'Card','Wallet','Cash'
)


