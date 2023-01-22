with
    source_fornecedores as (
        select 
            cast(supplier_id as	int) as id_fornecedor
            , cast(company_name as	string) as nome_do_fornecedor
            , cast(contact_name as	string) as nome_do_contato_fornecedor
            , cast(contact_title as	string) as titulo_do_contato_fornecedor
            , cast(address as	string) as endereco_fornecedor
            , cast(city as	string) as cidade_fornecedor
            , cast(region as	string) as regiao_fornecedor
            , cast(postal_code as	string) as cep_fornecedor
            , cast(country as	string) as pais_fornecedor
            , cast(phone as	string) as telefone_fornecedor
            , cast(fax as	string) as fax_fornecedor
            -- , homepage
        from {{ source('erp', 'suppliers') }}
    )

select *
from source_fornecedores