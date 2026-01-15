from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
from pathlib import Path
import duckdb
import os

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Define a raiz para a pasta 'include' do Astro.
# Certifique-se de que a pasta 'ibc' está dentro de 'include' no seu projeto local.
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DATA_ROOT = Path(AIRFLOW_HOME) / "include"

@dag(
    start_date=datetime(2024, 6, 1),
    schedule="@daily",
    catchup=False
)
def telecom_etl():

    @task
    def extract_files(input_dir_name: str) -> list[str]:
        # Monta o caminho: /usr/local/airflow/include/ibc
        input_path = DATA_ROOT / input_dir_name
        bronze_dir = DATA_ROOT / "telecom_data" / "bronze"
        
        # Cria pastas de saída
        bronze_dir.mkdir(parents=True, exist_ok=True)
        
        # --- DEBUG LOGS ---
        print(f"--- INICIANDO EXTRACT ---")
        print(f"Buscando arquivos em: {input_path}")
        
        if not input_path.exists():
            # Lista o que tem na raiz do include para ajudar a debugar
            print(f"ERRO: A pasta '{input_path}' não existe.")
            if DATA_ROOT.exists():
                print(f"Conteúdo de {DATA_ROOT}: {list(DATA_ROOT.glob('*'))}")
            raise FileNotFoundError(f"Diretório não encontrado: {input_path}")

        # Pega CSV e csv (case insensitive)
        csv_files = list(input_path.glob("*.csv")) + list(input_path.glob("*.CSV"))
        print(f"Arquivos encontrados: {[f.name for f in csv_files]}")

        if not csv_files:
            print("Nenhum CSV encontrado. Retornando lista vazia.")
            return []

        output_files = []
        
        for file in csv_files:
            try:
                print(f"Lendo: {file.name}")
                df = pd.read_csv(file, sep=";", encoding="latin1", engine="python", on_bad_lines="skip")
                
                output_file = bronze_dir / file.name
                df.to_csv(output_file, index=False)
                output_files.append(str(output_file))
            except Exception as e:
                print(f"Falha ao ler {file.name}: {e}")
                
        return output_files

    @task
    def validate_data(files: list[str]):
        if not files:
            print("Validacao pulada: Lista de arquivos vazia.")
            return []

        validated_files = []
        for file in files:
            file_path = Path(file)
            # Lê o arquivo que acabamos de salvar na bronze
            df = pd.read_csv(file_path)
            
            if df.isna().any().any():
                print(f"Corrigindo nulos em: {file_path.name}")
                df.fillna(0, inplace=True)
                df.to_csv(file_path, index=False)
            validated_files.append(str(file_path))
            return validated_files
    
    @task
    def load_to_duckdb(files: list[str]):
        if not files:
            print("Carga no DuckDB pulada: Lista de arquivos vazia.")
            return None

        db_path = DATA_ROOT / "telecom_data" / "warehouse" / "telecom.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Conectando ao DuckDB em: {db_path}")
        con = duckdb.connect(database=str(db_path), read_only=False)

        for file in files:
            file_path = Path(file)
            table_name = f"raw_{file_path.stem}".replace(" ", "_").lower()

            print(f"Criando tabela: {table_name}")
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{file_path}');
            """)
            
            # Validação rápida
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            print(f"Tabela {table_name} carregada com {count[0]} linhas.")

        con.close()
        return str(db_path)
    
    # --- FLUXO DE EXECUÇÃO ---
    
    # 1. Extração (Passando o argumento que faltava!)
    bronze_files = extract_files(input_dir_name="ibc")
    
    # 2. Validação
    validated_data = validate_data(bronze_files)
    
    # 3. Carga
    load_to_duckdb(validated_data)

telecom_etl()