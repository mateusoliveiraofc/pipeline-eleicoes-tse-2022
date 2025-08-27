from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sys
import os

sys.path.append('/opt/airflow/scripts')

from download_dados_origem import download_dados
from carga_postgres import CargaDados

# Configuração DAG
default_args = {
    'owner': 'squad_dados',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Definição da DAG
dag = DAG(
    'pipeline_eleicoes_2022',
    default_args=default_args,
    description='Pipeline de análise de dados eleitorais 2022',
    schedule_interval='@weekly',  
    max_active_runs=1,
    tags=['eleicoes', 'tse', 'dados']
)

def run_download():
    """Função para executar o download dos dados"""
    downloader = download_dados()
    downloader.run()

def run_load():
    """Função para executar a carga dos dados"""
    loader = CargaDados()
    loader.run()

# Download e ingestão dos dados do TSE
task_download = PythonOperator(
    task_id='download_dados_tse',
    python_callable=run_download,
    dag=dag,
    doc_md="""

    - Baixa o arquivo ZIP do site do TSE
    - Extrai os arquivos CSV 
    - Envia os dados brutos para o MinIO (Data Lake)
    """
)

# Carga dos dados para PostgreSQL
task_load = PythonOperator(
    task_id='carga_postgres',
    python_callable=run_load,
    dag=dag,
    doc_md="""

    - Lê os arquivos CSV do MinIO
    - Processa os dados em chunks para evitar estouro de memória
    - Carrega na tabela staging do PostgreSQL
    - Aplica limpeza básica e validações
    """
)

# Executa transformações dbt - Stg
task_dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --models staging',
    dag=dag,
    doc_md="""
    ### Transformações dbt - Staging Layer
    
    Executa os modelos de staging do dbt:
    - Limpeza e tipagem dos dados
    - Seleção de colunas relevantes
    - Filtros para cargos majoritários
    """
)

# Executa transformações dbt - Analytics
task_dbt_analytics = BashOperator(
    task_id='dbt_analytics', 
    bash_command='cd /opt/airflow/dbt && dbt run --models marts',
    dag=dag,
    doc_md="""
    ### Transformações dbt - Analytics Layer
    
    Executa os modelos analíticos do dbt:
    - Criação da tabela fato principal
    - Tabelas agregadas por nível (Brasil, Estado, Município)
    - Otimização para consultas analíticas
    """
)

# Testes de qualidade
task_dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test',
    dag=dag,
    doc_md="""

    Executa os testes do dbt para garantir qualidade dos dados:
    - Testes de integridade (not_null, unique)
    - Validações de domínio (accepted_values)
    - Testes customizados de negócio
    """
)

task_download >> task_load >> task_dbt_staging >> task_dbt_analytics >> task_dbt_test