with
    ordens as (
        select
            id_pedido
            , id_funcionario
            , id_cliente
            , id_transportadora
            , data_do_pedido
            , frete
            , destinatario
            , endereco_destinatario
            , cep_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , pais_destinatario
            , data_do_envio
            , data_requerida
        from {{ ref('stg_erp__ordens') }}
    )

    , ordem_detalhes as (
        select
           id_pedido 
            , id_produto
            , desconto
            , preco_por_unidade
            , quantidade
        from {{ ref('stg_erp__ordem_detalhes') }}
    )

    , joined as (
        select
            ordens.id_pedido 
            , ordens.id_funcionario
            , ordens.id_cliente
            , ordens.id_transportadora
            , ordem_detalhes.id_produto
            
            , ordem_detalhes.desconto
            , ordem_detalhes.preco_por_unidade
            , ordem_detalhes.quantidade
            , ordens.frete

            , ordens.data_do_pedido
            , ordens.data_do_envio
            , ordens.data_requerida
            
            , ordens.destinatario
            , ordens.endereco_destinatario
            , ordens.cep_destinatario
            , ordens.cidade_destinatario
            , ordens.regiao_destinatario
            , ordens.pais_destinatario
        from ordem_detalhes
        left join ordens on
            ordens.id_pedido = ordem_detalhes.id_pedido
    )

select *
from joined