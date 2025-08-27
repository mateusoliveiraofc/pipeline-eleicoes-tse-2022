import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CargaDados:
    def __init__(self):
        # Configuração PostgreSQL
        self.pg_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'eleicoes_dw',
            'user': 'airflow',
            'password': 'airflow123'
        }

        # MinIO
        self.minio_client = Minio(
            "minio:9000",
            access_key="admin",
            secret_key="admin123456",
            secure=False
        )

        # SQLAlchemy
        self.engine = create_engine(
            f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}@"
            f"{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
        )

    def criar_stg(self):
        """Cria a tabela staging se não existir"""
        criar_stg_sql = """
        CREATE TABLE IF NOT EXISTS staging.staging_eleicoes (
            nr_turno INTEGER,
            cd_eleicao INTEGER,
            sg_uf VARCHAR(2),
            cd_municipio INTEGER,
            nm_municipio VARCHAR(200),
            nu_zona INTEGER,
            nu_secao INTEGER,
            cd_cargo INTEGER,
            ds_cargo_pergunta VARCHAR(200),
            nr_votavel INTEGER,
            nm_votavel VARCHAR(200),
            qt_votos INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.engine.begin() as connection:
            connection.execute(text(criar_stg_sql))
        logger.info("Tabela staging criada")

    def list_csv_arquivos_minio(self):
        """Lista os arquivos CSV no MinIO"""
        objects = self.minio_client.list_objects(
            "raw-data", prefix="eleicoes_2022/", recursive=True
        )
        csv_files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
        logger.info(f"Encontrados {len(csv_files)} arquivos CSV no MinIO")
        return csv_files

    def _normaliza_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mapeia colunas equivalentes do TSE para um conjunto canônico.
        Retorna um novo DataFrame apenas com as colunas mapeadas e já renomeadas para minúsculas.
        """
        alias_map = {
            'NR_TURNO':           ['NR_TURNO'],
            'CD_ELEICAO':         ['CD_ELEICAO'],
            'SG_UF':              ['SG_UF'],
            'CD_MUNICIPIO':       ['CD_MUNICIPIO', 'CD_MUN_SGE', 'CD_MUNICIPIO_TSE'],
            'NM_MUNICIPIO':       ['NM_MUNICIPIO', 'NM_MUN_SGE'],
            'NU_ZONA':            ['NU_ZONA', 'NR_ZONA'],
            'NU_SECAO':           ['NU_SECAO', 'NR_SECAO'],
            'CD_CARGO':           ['CD_CARGO'],
            'DS_CARGO_PERGUNTA':  ['DS_CARGO_PERGUNTA', 'DS_CARGO'],
            'NR_VOTAVEL':         ['NR_VOTAVEL', 'NR_CANDIDATO'],
            'NM_VOTAVEL':         ['NM_VOTAVEL', 'NM_CANDIDATO'],
            'QT_VOTOS':           ['QT_VOTOS', 'QTDE_VOTOS', 'QT_VOTOS_NOMINAIS', 'QT_VOTOS_TOTAL']
        }

        cols_encontradas = {}
        for canon, candidatos in alias_map.items():
            for c in candidatos:
                if c in df.columns:
                    cols_encontradas[canon] = c
                    break  # pega o primeiro que existir

        if not cols_encontradas:
            return pd.DataFrame()

        df2 = df[list(cols_encontradas.values())].copy()
        df2.columns = [k.lower() for k in cols_encontradas.keys()]  # canônico em minúsculas
        return df2

    def limpa_valida_dados(self, df_chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza nomes de colunas (aliases TSE), valida e aplica tipos/filtros.
        """
        df = self._normaliza_colunas(df_chunk)
        if df.empty:
            return df

        # Converte qt_votos se existir
        if 'qt_votos' in df.columns:
            df['qt_votos'] = pd.to_numeric(df['qt_votos'], errors='coerce')

        # Dropna apenas no que EXISTE neste chunk
        subset_drop = [c for c in ['sg_uf', 'nm_municipio', 'qt_votos'] if c in df.columns]
        if subset_drop:
            df = df.dropna(subset=subset_drop)

        # Filtro de votos válidos, se existir a coluna
        if 'qt_votos' in df.columns:
            df = df[df['qt_votos'] > 0]

        # Ajusta tipos numéricos mais comuns (se existirem)
        for col in ['nr_turno', 'cd_eleicao', 'cd_municipio', 'nu_zona', 'nu_secao',
                    'cd_cargo', 'nr_votavel']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        return df

    @staticmethod
    def _detectar_encoding_e_sep(sample_bytes: bytes):
        """
        Detecta encoding e separador sem depender de decodificar previamente em utf-8.
        Heurística para CSVs do TSE: tenta encodings comuns e conta delimitadores.
        """
        candidate_encodings = ['utf-8', 'latin1', 'cp1252']
        melhor = None
        for enc in candidate_encodings:
            try:
                sample_bytes.decode(enc)
                melhor = enc
                break
            except UnicodeDecodeError:
                continue
        if melhor is None:
            melhor = 'latin1'  # fallback seguro para pt-BR

        s = sample_bytes.decode(melhor, errors='ignore')
        sep = ';' if s.count(';') >= s.count(',') else ','
        return melhor, sep

    def carregar_csv_postgres(self, csv_path: str, chunk_size: int = 50_000):
        """Carrega um arquivo CSV para PostgreSQL em chunks"""
        logger.info(f"Processando arquivo: {csv_path}")

        response = None
        try:
            # Baixar arquivo como bytes
            response = self.minio_client.get_object("raw-data", csv_path)
            csv_bytes = response.read()

            # detectar encoding e separador a partir de um pedaço
            sample = csv_bytes[:200_000]  # 200KB é suficiente pra header + algumas linhas
            encoding, sep = self._detectar_encoding_e_sep(sample)
            logger.info(f"Encoding detectado: {encoding} | Separador: {sep}")

            chunk_count = 0
            total_rows = 0

            # Ler em chunks
            for chunk in pd.read_csv(
                BytesIO(csv_bytes),
                chunksize=chunk_size,
                sep=sep,
                encoding=encoding,
                low_memory=False,
                na_values=['#NULO#', '', 'NaN'],
                on_bad_lines='skip'  # se houver alguma linha malformada, pula
            ):
                if chunk_count == 0:
                    logger.info(f"Header original do CSV ({csv_path}): {list(chunk.columns)}")

                chunk_count += 1
                clean = self.limpa_valida_dados(chunk)

                if not clean.empty:
                    clean.to_sql(
                        'staging_eleicoes',
                        self.engine,
                        schema='staging',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )

                    total_rows += len(clean)
                    logger.info(f"Chunk {chunk_count}: {len(clean)} linhas inseridas")

                del chunk, clean

            logger.info(f"Arquivo {csv_path} processado: {total_rows} linhas inseridas")

        except Exception as e:
            logger.error(f"Erro ao processar {csv_path}: {str(e)}")
            raise
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

    def run(self):
        """Executa o processo de carga completo"""
        try:
            self.criar_stg()
            arquivos = self.list_csv_arquivos_minio()

            for caminho in arquivos:
                self.carregar_csv_postgres(caminho)

            logger.info("Processo de carga concluído!")

        except Exception as e:
            logger.error(f"Erro no processo de carga: {str(e)}")
            raise


if __name__ == "__main__":
    loader = CargaDados()
    loader.run()
