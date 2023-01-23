with
    transportadoras as (
        select *
        from {{ ref('stg_erp__transportadoras') }}
    )

    , transformed as (
        select
            row_number() over (order by id_transportadora) as sk_transportadora
            , id_transportadora
            , nome_da_transportadora
            , telefone_da_transportadora
        from transportadoras
    )

select *
from transformed