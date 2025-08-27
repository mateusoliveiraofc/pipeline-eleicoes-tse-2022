-- Resultado agregado por candidato e estado (Presidente e Governador)
{{ config(materialized='table') }}

with fato_eleicao as (
    select * from {{ ref('fct_eleicao') }}
    where nome_cargo in ('Presidente', 'Governador')
),

resultado_por_estado as (
    select 
        turno,
        uf,
        nome_cargo,
        numero_candidato,
        nome_candidato,
        tipo_voto,
        sum(quantidade_votos) as total_votos_estado,
        count(distinct codigo_municipio) as municipios_com_votos,
        avg(quantidade_votos) as media_votos_por_municipio,
        max(updated_at) as updated_at
        
    from fato_eleicao
    group by 
        turno, uf, nome_cargo, numero_candidato, 
        nome_candidato, tipo_voto
),

metricas as (
    select 
        *,
        
        -- Percentual no estado (por cargo)
        round(
            (total_votos_estado * 100.0 / 
             sum(total_votos_estado) over (
                 partition by turno, uf, nome_cargo
             )
            ), 2
        ) as percentual_no_estado,
        
        -- Ranking no estado (por cargo)
        row_number() over (
            partition by turno, uf, nome_cargo 
            order by total_votos_estado desc
        ) as ranking_no_estado
        
    from resultado_por_estado
)

select 
    turno,
    uf,
    nome_cargo,
    ranking_no_estado,
    numero_candidato,
    nome_candidato,
    tipo_voto,
    total_votos_estado,
    percentual_no_estado,
    municipios_com_votos,
    round(media_votos_por_municipio, 0) as media_votos_por_municipio,
    updated_at
    
from metricas
order by turno, uf, nome_cargo, ranking_no_estado