import sys
import requests
import pandas as pd
import os 
import time
from databricks.connect import DatabricksSession



spark = DatabricksSession.builder.getOrCreate()

# funções auxiliares (fora de task)
def normalize_text(text):
    return text.lower().strip()

def match_title(user_input, anime):
    # Verifica se existe lista de títulos e itera sobre ela
    titles = anime.get("titles", [])
    return any(
        normalize_text(user_input) in normalize_text(t["title"])
        for t in titles
    )

def fetch_anime_id_by_name(anime_name: str) -> int:
    print(f"Buscando ID para: {anime_name}...")
    resp = requests.get(
        "https://api.jikan.moe/v4/anime",
        params={"q": anime_name, "limit": 10}
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])

    if not data:
        raise ValueError(f"Nenhum anime encontrado com o nome {anime_name}")

    for anime in data:
        if match_title(anime_name, anime):
            print(f"Anime encontrado: {anime['title']} (ID: {anime['mal_id']})")
            return anime["mal_id"]

    # Fallback para o primeiro resultado
    first_match = data[0]
    print(f"Match exato não encontrado. Usando: {first_match['title']} (ID: {first_match['mal_id']})")
    return first_match["mal_id"]

def fetch_jikan_characters(anime_name: str, path: str, max_characters: int = None) -> str:
    anime_id = fetch_anime_id_by_name(anime_name)
    
    print("Buscando personagens...")
    # O endpoint de characters NÃO tem paginação no Jikan v4, retorna tudo de uma vez
    resp = requests.get(f"https://api.jikan.moe/v4/anime/{anime_id}/characters")
    resp.raise_for_status()
    
    # Pega os dados
    items = resp.json().get("data", [])
    
    # Aplica o limite se foi solicitado
    if max_characters:
        items = items[:max_characters]

    os.makedirs(path, exist_ok=True)
    file_path = f"{path}/{anime_name}_characters.csv"
    
    # json_normalize é melhor aqui porque a resposta do Jikan é aninhada
    # (Ex: character.name, character.url, role)
    df = pd.json_normalize(items)
    
    df.to_csv(file_path, index=False)
    print(f"Arquivo salvo em: {file_path}")

    return file_path

def print_first_10_characters(csv_path: str):
    df = pd.read_csv(csv_path)
    print(df.head(10))



fetch_jikan_characters("One Punch Man","../bronze", max_characters=10)


def export_to_databricks(df, catalog_schema, table_name):
    df = spark.createDataFrame(df)
    full_table_name = f"{catalog_schema}.{table_name}"
    df.write.mode("overwrite").saveAsTable(f"{full_table_name}")
    print("feito")
    


os.makedirs("bronze", exist_ok=True)
df = pd.read_csv("bronze/One Punch Man_characters.csv")
export_to_databricks(df, "default", "bleach_characters")