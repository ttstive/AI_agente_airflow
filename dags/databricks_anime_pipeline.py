from airflow.decorators import dag, task
from datetime import datetime
import os

# --- 1. Funções Auxiliares (Lógica Pura) ---
# Estão aqui apenas como definições. As tasks vão chamá-las.

def normalize_text(text):
    return text.lower().strip()

def match_title(user_input, anime):
    titles = anime.get("titles", [])
    return any(
        normalize_text(user_input) in normalize_text(t["title"])
        for t in titles
    )

def fetch_anime_id_by_name(anime_name: str):
    import requests # Import local para segurança
    
    print(f"Buscando ID para: {anime_name}...")
    resp = requests.get(
        "https://api.jikan.moe/v4/anime",
        params={"q": anime_name, "limit": 10}
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])

    if not data:
        print(f"Aviso: Nenhum anime encontrado para {anime_name}")
        return None

    for anime in data:
        if match_title(anime_name, anime):
            return anime["mal_id"]

    first_match = data[0]
    return first_match["mal_id"]

def fetch_jikan_characters_logic(anime_name: str, path: str, max_characters: int = None) -> str:
    import requests
    import pandas as pd
    import time
    
    anime_id = fetch_anime_id_by_name(anime_name)
    
    if not anime_id:
        return None

    print(f"Buscando personagens (ID: {anime_id})...")
    # API do Jikan pode falhar se formos muito rápido, retry simples
    time.sleep(1) 
    
    resp = requests.get(f"https://api.jikan.moe/v4/anime/{anime_id}/characters")
    resp.raise_for_status()
    
    items = resp.json().get("data", [])
    
    if max_characters:
        items = items[:max_characters]

    os.makedirs(path, exist_ok=True)
    # Garante nome de arquivo sem espaços
    safe_name = anime_name.replace(" ", "_")
    file_path = os.path.join(path, f"{safe_name}_characters.csv")
    
    # json_normalize para achatar a estrutura aninhada da API
    df = pd.json_normalize(items)
    df.to_csv(file_path, index=False)
    
    print(f"Arquivo salvo em: {file_path}")
    return file_path


# --- 2. Definição do DAG ---

@dag(
    dag_id="anime_pipeline_consolidado_v1",
    start_date=datetime(2025, 1, 1),
    schedule=None, # Rodar manualmente para teste
    catchup=False,
    max_active_tasks=1 # Importante para não bloquear a API do Jikan
)
def anime_pipeline_consolidado():

    @task
    def get_anime_list():
        # Lista de entradas
        return ["Naruto", "Bleach", "One Punch Man", "Dragon Ball Z"]

    @task
    def extract_task(anime_name: str):
        import os
        
        # Define onde salvar (pasta 'bronze_data' ao lado deste arquivo DAG)
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(dag_folder, 'bronze_data')
        
        # Chama a função auxiliar definida no topo
        # max_characters=10 conforme seu pedido
        path_generated = fetch_jikan_characters_logic(anime_name, output_path, max_characters=10)
        
        if not path_generated:
            print(f"Pulo: Não foi possível gerar dados para {anime_name}")
            return None
            
        return path_generated

    @task
    def databricks_export_task(file_path: str, catalog_schema: str):
        import pandas as pd
        import os
        # Importação crítica feita DENTRO da task
        from databricks.connect import DatabricksSession

        if not file_path or not os.path.exists(file_path):
            print(f"Arquivo não existe ou é nulo: {file_path}")
            return

        print(f"Iniciando exportação para o arquivo: {file_path}")

        # 1. Leitura com Pandas
        # fillna("") é crucial pois Spark não gosta de NaN misturado com strings
        df_pandas = pd.read_csv(file_path).fillna("")
        
        # 2. Definição do nome da tabela (Ex: Naruto_characters.csv -> tb_naruto)
        filename = os.path.basename(file_path)
        clean_name = filename.split('_characters')[0].lower().replace(" ", "_")
        
        # Remove caracteres especiais se houver
        clean_name = "".join(e for e in clean_name if e.isalnum() or e == "_")
        
        full_table_name = f"{catalog_schema}.tb_{clean_name}"

        # 3. Sessão Databricks (Só inicia AGORA, no worker)
        print("Criando sessão Databricks...")
        spark = DatabricksSession.builder.getOrCreate()
        
        # 4. Conversão e Escrita
        print(f"Convertendo DataFrame e salvando em {full_table_name}...")
        try:
            sdf = spark.createDataFrame(df_pandas)
            sdf.write.mode("overwrite").saveAsTable(full_table_name)
            print("Sucesso!")
        except Exception as e:
            print(f"Erro ao salvar no Databricks: {e}")
            raise e

    # --- 3. Fluxo de Execução ---
    
    # Pega a lista
    lista_animes = get_anime_list()
    
    # Gera os arquivos CSV (um para cada anime)
    caminhos_csv = extract_task.expand(anime_name=lista_animes)
    
    # Envia para o Databricks
    # .partial fixa o argumento 'catalog_schema'
    # .expand mapeia a lista de arquivos gerados
    databricks_export_task.partial(catalog_schema="main.default").expand(file_path=caminhos_csv)

# Instancia o DAG
dag = anime_pipeline_consolidado()