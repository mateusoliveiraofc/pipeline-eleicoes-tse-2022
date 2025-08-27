import os
import zipfile
import requests
import pandas as pd
from minio import Minio
from io import BytesIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class download_dados:
    def __init__(self):
        self.tse_url = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip"
        # Cliente MinIO
        self.minio_client = Minio(
            "minio:9000",
            access_key="admin",
            secret_key="admin123456",
            secure=False
        )
        
        #bucket
        self.bucket_name = "raw-data"
        
    def criacao_bucket(self):
        """Cria o bucket no MinIO se não existir"""
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' criado")
        else:
            logger.info(f"Bucket '{self.bucket_name}' já existe")
    
    def download_dados_tse(self):
        try:
            response = requests.get(self.tse_url, stream=True)
            response.raise_for_status()
            
            # Salvar o arquivo zip temporariamente
            zip_path = "/tmp/tse_data.zip"
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info("Download concluído!")
            return zip_path
            
        except Exception as e:
            logger.error(f"Erro no download: {str(e)}")
            raise
    
    def extracao_arquivos_csv(self, zip_path):
        """Extrai os arquivos CSV do ZIP"""
        logger.info("Extraindo arquivos CSV...")
        
        csv_arquivos = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.csv'):
                    # Extrair para memória
                    csv_dados = zip_ref.read(file_name)
                    csv_arquivos.append({
                        'name': file_name,
                        'data': csv_dados
                    })
        
        logger.info(f"Encontrados {len(csv_arquivos)} arquivos CSV")
        return csv_arquivos
    
    def carregamento_para_minio(self, csv_arquivos):
        logger.info("Enviando arquivos para o MinIO...")
        
        for csv_arquivos in csv_arquivos:
            try:
                # Converter dados para BytesIO
                data_stream = BytesIO(csv_arquivos['data'])
                
                # Upload para MinIO
                self.minio_client.put_object(
                    self.bucket_name,
                    f"eleicoes_2022/{csv_arquivos['name']}",
                    data_stream,
                    length=len(csv_arquivos['data']),
                    content_type='text/csv'
                )
                
                logger.info(f"Arquivo {csv_arquivos['name']} enviado com sucesso")
                
            except Exception as e:
                logger.error(f"Erro ao enviar {csv_arquivos['name']}: {str(e)}")
                raise
    
    def run(self):
        """Executa todo o processo de download e upload"""
        try:
            # Criar bucket
            self.criacao_bucket()
            
            # Download dos dados
            zip_path = self.download_dados_tse()
            
            # Extrair CSVs
            csv_files = self.extracao_arquivos_csv(zip_path)
            
            # Upload para MinIO
            self.carregamento_para_minio(csv_files)
            
            # Limpar arquivo temporário
            if os.path.exists(zip_path):
                os.remove(zip_path)
            
            logger.info("Processo completo! Dados disponíveis no MinIO")
            
        except Exception as e:
            logger.error(f"Erro no processo: {str(e)}")
            raise

if __name__ == "__main__":
    downloader = download_dados()
    downloader.run()