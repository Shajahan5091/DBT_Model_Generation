
    
    

select
    shipment_id as unique_field,
    count(*) as n_records

from raw_db.raw_data.shipments
where shipment_id is not null
group by shipment_id
having count(*) > 1


