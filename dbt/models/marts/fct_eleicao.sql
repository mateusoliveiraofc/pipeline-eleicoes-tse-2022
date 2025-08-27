{{ config(materialized='table') }}

with eleicoes_staging as (
    select * from {{ ref('stg_eleicoes') }}
),

fato_principal as (
    select 
        turno,
        uf,
        codigo_municipio,
        nome_municipio,
        zona,
        secao,
        codigo_cargo,
        nome_cargo,
        numero_candidato,
        nome_candidato,
        tipo_voto,
        quantidade_votos,
        codigo_eleicao,
        created_at,
        current_timestamp as updated_at
        
    from eleicoes_staging
),

metricas_enriquecidas as (
    select 
        *,
        
        -- Percentual de votos por município (por cargo)
        round(
            (quantidade_votos * 100.0 / 
             sum(quantidade_votos) over (
                 partition by uf, codigo_municipio, nome_cargo, turno
             )
            ), 2
        ) as percentual_votos_municipio,
        
        -- Ranking do candidato no município (por cargo) 
        row_number() over (
            partition by uf, codigo_municipio, nome_cargo, turno
            order by quantidade_votos desc
        ) as ranking_municipio
        
    from fato_principal
)

select * from metricas_enriquecidas