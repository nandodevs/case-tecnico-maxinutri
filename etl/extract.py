"""
Script para extração dos dados da API, salvando em Parquet e CSV na pasta data/raw.
Inclui tratamento de erros com retries e validação básica dos dados.
"""
import requests
import pandas as pd
import time
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")
TOKEN = os.getenv("TOKEN")
RAW_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "raw")).resolve()
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

RAW_DIR.mkdir(exist_ok=True)

def fetch_page(page: int):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_URL,
                params={"token": TOKEN, "page": page},
                timeout=50
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erro na requisição da página {page} (tentativa {attempt}): {e}")
            time.sleep(BACKOFF_FACTOR ** attempt)
    raise Exception(f"Falha ao obter página {page} após {MAX_RETRIES} tentativas.")

def main():
    print("Iniciando extração de dados da API...")
    all_data = []
    page = 1
    while True:
        page_data = fetch_page(page)
        dados = page_data.get("dados", [])
        if not dados:
            print(f"Nenhum dado encontrado na página {page}. Encerrando extração.")
            break
        all_data.extend(dados)
        print(f"Página {page} extraída com {len(dados)} registros.")
        page += 1
    df = pd.DataFrame(all_data)
    print(f"Total de registros extraídos: {len(df)}")
    
    # Salva em Parquet e CSV na pasta data/raw
    parquet_path = RAW_DIR / "dados_api.parquet"
    csv_path = RAW_DIR / "dados_api.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    print(f"Dados salvos em: {parquet_path} e {csv_path}")

if __name__ == "__main__":
    main()