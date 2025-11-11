
    
    

with all_values as (

    select
        is_vip_flag as value_field,
        count(*) as n_records

    from raw_db.raw_data.customers
    group by is_vip_flag

)

select *
from all_values
where value_field not in (
    'Y','N'
)


