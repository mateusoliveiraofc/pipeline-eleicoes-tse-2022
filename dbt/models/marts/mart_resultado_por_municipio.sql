-- Resultado agregado por candidato, estado e município (todos os cargos)
{{ config(materialized='table') }}

with fato_eleicao as (
    select * from {{ ref('fct_eleicao') }}
),

resultado_por_municipio as (
    select 
        turno,
        uf,
        codigo_municipio,
        nome_municipio,
        nome_cargo,
        numero_candidato,
        nome_candidato,
        tipo_voto,
        sum(quantidade_votos) as total_votos_municipio,
        count(distinct zona) as total_zonas,
        count(distinct secao) as total_secoes,
        avg(quantidade_votos) as media_votos_por_secao,
        min(quantidade_votos) as min_votos_secao,
        max(quantidade_votos) as max_votos_secao,
        max(updated_at) as updated_at
        
    from fato_eleicao
    group by 
        turno, uf, codigo_municipio, nome_municipio, nome_cargo,
        numero_candidato, nome_candidato, tipo_voto
),

rankings as (
    select 
        *,
        
        -- Percentual no município (por cargo)
        round(
            (total_votos_municipio * 100.0 / 
             sum(total_votos_municipio) over (
                 partition by turno, codigo_municipio, nome_cargo
             )
            ), 2
        ) as percentual_no_municipio,
        
        -- Ranking no município (por cargo)
        row_number() over (
            partition by turno, codigo_municipio, nome_cargo 
            order by total_votos_municipio desc
        ) as ranking_no_municipio,
        
        -- Identificar vencedor no município
        case 
            when row_number() over (
                partition by turno, codigo_municipio, nome_cargo 
                order by total_votos_municipio desc
            ) = 1 and tipo_voto = 'Nominal'
            then true 
            else false 
        end as vencedor_municipio
        
    from resultado_por_municipio
)

select 
    turno,
    uf,
    codigo_municipio,
    nome_municipio,
    nome_cargo,
    ranking_no_municipio,
    numero_candidato,
    nome_candidato,
    tipo_voto,
    total_votos_municipio,
    percentual_no_municipio,
    vencedor_municipio,
    total_zonas,
    total_secoes,
    round(media_votos_por_secao, 1) as media_votos_por_secao,
    min_votos_secao,
    max_votos_secao,
    updated_at
    
from rankings
order by turno, uf, nome_municipio, nome_cargo, ranking_no_municipio