FROM apache/airflow:2.7.0-python3.10

USER root

# Instalar dependências
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    && apt-get clean

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN pip install dbt-postgres==1.6.0

RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/scripts