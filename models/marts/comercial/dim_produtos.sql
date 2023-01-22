with
    categorias as (
        select *
        from {{ ref('stg_erp__categorias') }}
    )
    
    , fornecedores as (
        select *
        from {{ ref('stg_erp__fornecedores') }}
    )

    , produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )
    
    , uniao_tabelas as (
        select
            produtos.id_produto
            , produtos.id_categoria
            , produtos.id_fornecedor
            , produtos.nome_produto
            , produtos.quantidade_por_unidade
            , produtos.preco_por_unidade
            , produtos.unidades_em_estoque
            , produtos.unidades_por_pedido
            , produtos.level_de_pedido
            , produtos.is_discontinuado
            , categorias.nome_categoria
            , categorias.descricao_categoria
            , fornecedores.nome_do_fornecedor
            , fornecedores.nome_do_contato_fornecedor
            , fornecedores.titulo_do_contato_fornecedor
            , fornecedores.endereco_fornecedor
            , fornecedores.cidade_fornecedor
            , fornecedores.regiao_fornecedor
            , fornecedores.cep_fornecedor
            , fornecedores.pais_fornecedor
            , fornecedores.telefone_fornecedor
            , fornecedores.fax_fornecedor
        from produtos
        left join categorias on produtos.id_categoria = categorias.id_categoria
        left join fornecedores on produtos.id_fornecedor = fornecedores.id_fornecedor
    )

    , transformacoes as (
        select
            row_number() over (order by id_produto) as sk_produto -- eu estou ordenando a tabela pelo id_produto, o row_number cria uma chave única sequencial (1, 2, 3...) e chamando isso tudo de sk_produto
            , *
        from uniao_tabelas
    )

select *
from transformacoes