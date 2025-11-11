
    
    

with all_values as (

    select
        stock_status as value_field,
        count(*) as n_records

    from DEMO_DB.STAGING.inventory_summary
    group by stock_status

)

select *
from all_values
where value_field not in (
    'IN_STOCK','OUT_OF_STOCK'
)


