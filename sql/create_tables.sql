-- Executado automaticamente quando o container PostgreSQL sobe

-- Criar extensões úteis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Criar schemas para organização
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Tabela para armazenar metadados de execução
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(20),
    records_processed INTEGER,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT
);

-- Função para log de execuções
CREATE OR REPLACE FUNCTION log_pipeline_execution(
    p_dag_id VARCHAR(100),
    p_task_id VARCHAR(100),
    p_status VARCHAR(20),
    p_records_processed INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO pipeline_execution_log (
        dag_id, task_id, execution_date, status, 
        records_processed, error_message
    )
    VALUES (
        p_dag_id, p_task_id, CURRENT_TIMESTAMP, p_status,
        p_records_processed, p_error_message
    )
    RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- View para monitoramento das execuções
CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT 
    dag_id,
    task_id,
    execution_date,
    status,
    records_processed,
    EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds,
    error_message
FROM pipeline_execution_log 
ORDER BY execution_date DESC
LIMIT 100;

-- Comentários das tabelas
COMMENT ON TABLE pipeline_execution_log IS 'Log de execuções do pipeline de dados';
COMMENT ON VIEW v_pipeline_status IS 'Status das execuções recentes do pipeline';