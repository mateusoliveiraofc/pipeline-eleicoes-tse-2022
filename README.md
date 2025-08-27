# Pipeline de Análise Eleições 2022 - TSE

## Sobre o Projeto

Este projeto processa dados eleitorais do TSE (Tribunal Superior Eleitoral) de 2022, criando um pipeline completo que vai desde o download dos dados brutos até análises agregadas por município, estado e nível nacional.

## Como Rodar

### Pré-requisitos

- Docker Desktop ou Docker Engine + Docker Compose
- Pelo menos 8GB de RAM
- Conexão com internet para download dos dados

### Instalação

1. **Clone/baixe os arquivos do projeto**
```bash
# Crie uma pasta para o projeto
mkdir pipeline-eleicoes-2022
cd pipeline-eleicoes-2022

# Copie todos os arquivos do projeto para esta pasta
```

2. **Execute o pipeline**
```bash
# Subir todos os serviços (primeira vez demora um pouco)
docker compose up -d

# Acompanhar os logs (opcional)
docker compose logs -f
```

3. **Aguardar inicialização**

O Airflow demora uns 2-3 minutos para ficar pronto. Você vai saber que está pronto quando conseguir acessar http://localhost:8080.

### Acessando os Serviços

**Airflow** (Interface principal)
- URL: http://localhost:8080
- Login: admin / admin123

**MinIO** (Armazenamento de arquivos)  
- URL: http://localhost:9001
- Login: admin / admin123456

**PostgreSQL** (Banco de dados)
- Host: localhost:5432
- Database: eleicoes_dw  
- User/Pass: airflow / airflow123

## Arquitetura do Pipeline

### Fluxo dos Dados

```
TSE (fonte) → MinIO (raw) → PostgreSQL (staging) → dbt (transformação) → PostgreSQL (marts)
```

### Etapas do Pipeline

1. **Download**: Baixa CSVs de boletins de urna por estado do portal do TSE
2. **Carga**: Carrega dados em chunks para não estourar memória 
3. **Staging**: Limpa e padroniza os dados brutos
4. **Transformação**: Cria tabela fato e agregações
5. **Testes**: Valida qualidade dos dados

### Estrutura das Tabelas

**Staging**: `stg_eleicoes` - Dados limpos e filtrados
**Fato**: `fct_eleicao` - Grão: votos por candidato/município/cargo  
**Marts**: 
- `mart_resultado_geral_br` - Agregado nacional (Presidente)
- `mart_resultado_por_estado` - Por estado (Presidente + Governador)  
- `mart_resultado_por_municipio` - Por município (todos os cargos)

## Executando o Pipeline

### Primeira Execução

1. Acesse http://localhost:8080 (Airflow)
2. Encontre a DAG `pipeline_eleicoes_2022`
3. Ative a DAG (toggle no canto esquerdo)
4. Clique em "Trigger DAG" para executar manualmente

### Monitoramento

A execução completa leva cerca de 30-45 minutos dependendo da velocidade de download. Você pode acompanhar:

- **Status das tasks** na interface do Airflow
- **Arquivos baixados** no MinIO 
- **Dados carregados** conectando no PostgreSQL

## Consultas Úteis

```sql
-- Top 10 candidatos nacionais (Presidente)
SELECT nome_candidato, total_votos, percentual_nacional 
FROM mart_resultado_geral_br 
WHERE turno = 2 AND tipo_voto = 'Nominal'
ORDER BY ranking_nacional LIMIT 10;

-- Resultado por estado para Presidente
SELECT uf, nome_candidato, total_votos_estado, percentual_no_estado
FROM mart_resultado_por_estado 
WHERE nome_cargo = 'Presidente' AND turno = 2
ORDER BY uf, ranking_no_estado;

-- Municípios onde um candidato específico ganhou
SELECT uf, nome_municipio, total_votos_municipio 
FROM mart_resultado_por_municipio
WHERE nome_candidato LIKE '%SEU_CANDIDATO%' AND vencedor_municipio = true;
```

## Problemas que Encontrei

### Airflow no Docker
Configurar o Airflow via Docker deu algumas dores de cabeça com as chaves secretas. A documentação oficial é meio confusa nisso. 

### Processamento de dados grandes
Os arquivos do TSE são grandes. Implementei processamento em chunks de 50k linhas, mas dependendo da máquina pode precisar ajustar. Se estiver dando pau de memória, diminua esse número no script de carga.

## Estrutura do Projeto

```
projeto/
├── docker-compose.yml       # Configuração dos serviços
├── airflow/                 # Configuração do Airflow
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── pipeline_eleicoes.py
├── dbt/                     # Transformações SQL
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       └── marts/
├── scripts/                 # Scripts Python
│   ├── download_dados_origem.py
│   └── carga_postgres.py
└── sql/
    └── create_tables.sql
```

## Observações Técnicas

### Utilização de IA 
Utilizei IA para configurar a parte do minio no docker_compose, na parte do mapeamento dos campos da tabela no script carga_postgre usei para consegui mapear os campos que mudavam o nome de acordo com arquivo exemplo ['QT_VOTOS', 'QTDE_VOTOS', 'QT_VOTOS_NOMINAIS', 'QT_VOTOS_TOTAL'] e formatação do README.
