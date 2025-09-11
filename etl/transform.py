"""
Script para transformação dos dados extraídos.
Lê dados brutos de data/raw, realiza tratamento e salva em data/processed.
"""
import pandas as pd
from pathlib import Path
import os

RAW_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "raw")).resolve()
PROCESSED_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "processed")).resolve()
PROCESSED_DIR.mkdir(exist_ok=True)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Normalização de colunas de texto
    if "customer_city" in df.columns:
        df["customer_city"] = df["customer_city"].astype(str).str.lower().str.strip()
    if "customer_state" in df.columns:
        df["customer_state"] = df["customer_state"].astype(str).str.upper().str.strip()

    # Conversão de colunas de data
    date_cols = ["order_approved_at", "review_creation_date", "review_answer_timestamp"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Preencher comentários nulos
    if "review_comment_message" in df.columns:
        df["review_comment_message"] = df["review_comment_message"].fillna("Sem comentários")

    # Excluir linhas sem dados essenciais
    critical_cols = ["order_id", "product_id", "price", "freight_value", "order_approved_at"]
    df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

    # Excluir linhas que têm apenas comentário, mas não têm dados de produto/pedido
    if "product_id" in df.columns:
        df = df[df["product_id"].notna() & (df["product_id"].astype(str) != "")]

    # Excluir linhas que não têm nenhum valor em colunas críticas (linhas de lixo)
    df = df.dropna(how="all")

    # Remover duplicados
    if "order_id" in df.columns and "product_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id", "product_id"])

    return df

def main():
    parquet_path = RAW_DIR / "dados_api.parquet"
    csv_path = RAW_DIR / "dados_api.csv"

    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        raise FileNotFoundError("Arquivo de dados brutos não encontrado em data/raw.")

    df = transform(df)

    # Salvar dados tratados
    processed_parquet = PROCESSED_DIR / f"dados_tratados.parquet"
    processed_csv = PROCESSED_DIR / f"dados_tratados.csv"
    df.to_parquet(processed_parquet, index=False)
    df.to_csv(processed_csv, index=False, encoding="utf-8-sig")
    print(f"Dados tratados salvos em: {processed_parquet} e {processed_csv}")

if __name__ == "__main__":
    main()
