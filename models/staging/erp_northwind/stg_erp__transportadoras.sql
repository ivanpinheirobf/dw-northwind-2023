with
    source_transportadoras as (
        select 
            cast(shipper_id	as int)	as id_transportadora
            , cast(company_name	as string)	as nome_da_transportadora
            , cast(phone	as string)	as telefone_da_transportadora
        from {{ source('erp', 'shippers') }}
    )

select *
from source_transportadoras