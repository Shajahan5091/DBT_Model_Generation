
    
    

select
    shipment_id as unique_field,
    count(*) as n_records

from demo_db.raw.raw_shipments
where shipment_id is not null
group by shipment_id
having count(*) > 1


