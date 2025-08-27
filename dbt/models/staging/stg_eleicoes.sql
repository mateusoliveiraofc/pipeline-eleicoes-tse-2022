{{ config(materialized='table') }}

with origem_dados as (
    select 
        nr_turno,
        cd_eleicao,
        sg_uf,
        cd_municipio,
        nm_municipio,
        nu_zona,
        nu_secao,
        cd_cargo,
        ds_cargo_pergunta,
        nr_votavel,
        nm_votavel,
        qt_votos,
        created_at
    from {{ source('staging', 'staging_eleicoes') }}
    where qt_votos > 0
),

dados_clean as (
    select 
        nr_turno::integer               as turno,
        cd_eleicao::integer             as codigo_eleicao,
        upper(trim(sg_uf))              as uf,
        cd_municipio::integer           as codigo_municipio,
        trim(nm_municipio)              as nome_municipio,
        nu_zona::integer                as zona,
        nu_secao::integer               as secao,
        cd_cargo::integer               as codigo_cargo,
        trim(ds_cargo_pergunta)         as cargo,
        nr_votavel::integer             as numero_candidato,
        trim(nm_votavel)                as nome_candidato,
        qt_votos::integer               as quantidade_votos,
        created_at
    from origem_dados
    where sg_uf is not null 
      and nm_municipio is not null
      and cd_cargo is not null
      and qt_votos is not null
      and qt_votos > 0
      and cd_cargo in ({{ var('cargos_majoritarios') | join(',') }})  -- 1,3,5
),

final as (
    select 
        *,
        case 
            when numero_candidato = 95 then 'Branco'
            when numero_candidato = 96 then 'Nulo' 
            else 'Nominal'
        end as tipo_voto,
        case 
            when codigo_cargo = 1 then 'Presidente'
            when codigo_cargo = 3 then 'Governador'
            when codigo_cargo = 5 then 'Senador'
            else 'Outro'
        end as nome_cargo
    from dados_clean
)

select * from final
