-- Resultado agregado por candidato no Brasil (apenas Presidente)
{{ config(materialized='table') }}

with fato_eleicao as (
    select * from {{ ref('fct_eleicao') }}
    where nome_cargo = 'Presidente'  -- Apenas eleição presidencial
),

resultado_brasil as (
    select 
        turno,
        nome_cargo,
        numero_candidato,
        nome_candidato,
        tipo_voto,
        sum(quantidade_votos) as total_votos,
        count(distinct codigo_municipio) as municipios_com_votos,
        count(distinct uf) as estados_com_votos,
        max(updated_at) as updated_at    
    from fato_eleicao
    group by 
        turno, nome_cargo, numero_candidato, 
        nome_candidato, tipo_voto
),

percentuais as (
    select 
        *,
        round(
            (total_votos * 100.0 / sum(total_votos) over (partition by turno)), 
            2
        ) as percentual_nacional,
        row_number() over (
            partition by turno 
            order by total_votos desc
        ) as ranking_nacional
        
    from resultado_brasil
)

select 
    turno,
    ranking_nacional,
    numero_candidato,
    nome_candidato,
    tipo_voto,
    total_votos,
    percentual_nacional,
    municipios_com_votos,
    estados_com_votos,
    updated_at
    
from percentuais
order by turno, ranking_nacional