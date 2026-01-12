from airflow.decorators import dag, task
from airflow.sdk import Variable
from datetime import datetime
import requests
import pandas as pd
import os
import re
import time
from airflow.providers.databricks.hooks.databricks import DatabricksHook
# --- Lógica de Negócio (Reutilizada) ---
def normalize_text(text):
    return text.lower().strip()

def match_title(user_input, anime):
    titles = anime.get("titles", [])
    return any(normalize_text(user_input) in normalize_text(t["title"]) for t in titles)

def fetch_anime_id_by_name(anime_name: str) -> int:
    # Pequeno sleep para respeitar a API entre chamadas de ID
    time.sleep(1) 
    resp = requests.get("https://api.jikan.moe/v4/anime", params={"q": anime_name, "limit": 10})
    
    if resp.status_code == 429:
        raise Exception("Erro 429: Rate Limit atingido. Diminua a concorrência.")
        
    resp.raise_for_status()
    data = resp.json().get("data", [])
    
    if not data:
        print(f"Anime {anime_name} não encontrado, pulando...")
        return None

    for anime in data:
        if match_title(anime_name, anime):
            return anime["mal_id"]
    return data[0]["mal_id"]

def fetch_jikan_characters_logic(anime_name: str, path: str, max_characters: int = None) -> str:
    anime_id = fetch_anime_id_by_name(anime_name)
    
    if not anime_id:
        return None

    print(f"Buscando personagens para ID: {anime_id}...")
    time.sleep(1) # Sleep de segurança para a API
    resp = requests.get(f"https://api.jikan.moe/v4/anime/{anime_id}/characters")
    resp.raise_for_status()
    
    items = resp.json().get("data", [])
    if max_characters:
        items = items[:max_characters]

    os.makedirs(path, exist_ok=True)
    # Nome do arquivo dinâmico baseado no anime
    file_path = os.path.join(path, f"{anime_name}_characters.csv")
    
    df = pd.json_normalize(items)
    df.to_csv(file_path, index=False)
    return file_path
# ----------------------------------------

@dag(
    dag_id="exec_anime_pipeline_multiplo",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    # CONCEITO CRÍTICO: Limita quantas tasks rodam ao mesmo tempo 
    # para não derrubar a API gratuita do Jikan (max 1 por vez para segurança)
    max_active_tasks=1,
    tags=["anime", "ETL", "Dynamic"]
)
def exec_anime_pipeline_multiplo():

    # Tarefa 1: Definir a lista de animes (Poderia vir de um banco de dados ou arquivo)
    @task
    def get_anime_list():
        return ["Boruto"]

    # Tarefa 2: Processar UM anime (será mapeada)
    @task
    def exec_job(anime_name: str):
        # Caminho relativo à pasta de DAGs (solução compatível com Docker e Local)
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(dag_folder, 'bronze_output')
        
        max_characters = 10

        print(f"Processando: {anime_name}")
        file_generated = fetch_jikan_characters_logic(anime_name, output_path, max_characters)
        
        if file_generated:
            print(f"Sucesso: {file_generated}")
        return file_generated
    



    
    lista_animes = get_anime_list()
    
    # O comando .expand() pega a lista e cria uma task para cada item
    exec_job.expand(anime_name=lista_animes)
        
        
pipeline = exec_anime_pipeline_multiplo()