"""
Script otimizado para transformação dos dados extraídos.
Converte automaticamente colunas para tipos corretos e remove linhas vazias.
Lê dados brutos de data/raw, realiza tratamento e salva em data/processed.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "raw")).resolve()
PROCESSED_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "processed")).resolve()
PROCESSED_DIR.mkdir(exist_ok=True)

# Tipos sugeridos para colunas conhecidas
DTYPE_MAPPING = {
    "order_id": "string",
    "customer_id": "string",
    "product_id": "string",
    "seller_id": "string",

    "customer_unique_id": "string",
    "customer_zip_code_prefix": "string",
    "customer_city": "string",
    "customer_state": "category",

    "product_category_name": "category",
    "product_name_lenght": "Int16",
    "product_description_lenght": "Int32",
    "product_photos_qty": "Int8",
    "product_weight_g": "float32",
    "product_length_cm": "float32",
    "product_height_cm": "float32",
    "product_width_cm": "float32",

    "order_item_id": "Int8",
    "order_status": "category",
    "price": "float32",
    "freight_value": "float32",

    "seller_zip_code_prefix": "string",
    "seller_city": "string",
    "seller_state": "category",

    "payment_sequential": "Int8",
    "payment_type": "category",
    "payment_installments": "Int8",
    "payment_value": "float32",

    "review_id": "string",
    "review_score": "Int8",
    "review_comment_title": "string",
    "review_comment_message": "string",

    "geolocation_zip_code_prefix": "string",
    "geolocation_lat": "float32",
    "geolocation_lng": "float32",
    "geolocation_city": "string",
    "geolocation_state": "category",
}

# Colunas de datas
DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "review_creation_date",
    "review_answer_timestamp",
]

def detect_and_clean_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas completamente vazias e linhas com IDs críticos inválidos.
    Preserva todas as colunas originais.
    """
    initial_rows = len(df)
    logger.info(f"Linhas iniciais: {initial_rows}")

    # 1. Remover linhas totalmente vazias
    df = df.dropna(how="all")

    # 2. Validar apenas IDs que existirem
    for col in ["order_id", "customer_id", "product_id"]:
        if col in df.columns:
            before = len(df)
            df = df[df[col].notna() & (df[col].astype(str).str.strip().str.len() > 0)]
            logger.info(f"{col}: removidas {before - len(df)} linhas inválidas")

    # 3. Deduplicar somente se tiver order_id + product_id
    if "order_id" in df.columns and "product_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["order_id", "product_id"])
        logger.info(f"Duplicatas removidas: {before - len(df)}")

    logger.info(f"Linhas finais: {len(df)} (removidas {initial_rows - len(df)})")
    return df

def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza colunas de texto sem descartar informações."""
    text_columns = [
        "customer_city",
        "seller_city",
        "geolocation_city",
        "product_category_name",
        "review_comment_title",
        "review_comment_message",
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(["nan", "NaN", "none", "None", "", "null"], np.nan)
            if "city" in col:
                df[col] = df[col].str.title()
            elif col == "product_category_name":
                df[col] = df[col].str.lower()
    return df

def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Valida colunas numéricas dentro de limites plausíveis."""
    numeric_columns = {
        "price": (0, 100000),
        "freight_value": (0, 10000),
        "payment_value": (0, 100000),
        "product_weight_g": (0, 50000),
        "product_length_cm": (0, 200),
        "product_height_cm": (0, 200),
        "product_width_cm": (0, 200),
        "product_photos_qty": (0, 50),
        "review_score": (1, 5),
        "payment_installments": (1, 24),
    }

    for col, (min_val, max_val) in numeric_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where((df[col] >= min_val) & (df[col] <= max_val), np.nan)
    return df

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas de data para datetime válido."""
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].where(
                (df[col].dt.year >= 2000) & (df[col].dt.year <= 2030), np.nan
            )
    return df

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica tipos otimizados para colunas conhecidas, preservando o resto."""
    for col in df.columns:
        if col in DTYPE_MAPPING:
            target_dtype = DTYPE_MAPPING[col]
            try:
                if target_dtype == "category":
                    df[col] = df[col].astype("string").astype("category")
                else:
                    df[col] = df[col].astype(target_dtype)
            except Exception as e:
                logger.warning(f"Falha ao converter {col} para {target_dtype}: {e}")
    return df

def generate_data_quality_report(df: pd.DataFrame) -> None:
    """Relatório resumido de qualidade dos dados."""
    logger.info("=== RELATÓRIO DE QUALIDADE ===")
    logger.info(f"Registros: {len(df):,}, Colunas: {len(df.columns)}")

    dtype_counts = df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        logger.info(f"{dtype}: {count} colunas")

    null_counts = df.isnull().sum().sort_values(ascending=False).head(10)
    for col, null_count in null_counts.items():
        if null_count > 0:
            perc = (null_count / len(df)) * 100
            logger.info(f"{col}: {null_count:,} ({perc:.1f}%)")

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Executa o pipeline garantindo que nenhuma coluna seja perdida."""
    logger.info("=== INICIANDO TRANSFORMAÇÃO ===")
    logger.info(f"Entrada: {len(df)} registros, {len(df.columns)} colunas")

    original_cols = df.columns.tolist()

    df = detect_and_clean_empty_rows(df)
    df = clean_text_columns(df)
    df = clean_numeric_columns(df)
    df = convert_date_columns(df)
    df = optimize_dtypes(df)

    # Reintroduzir colunas perdidas
    for col in original_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Reordenar colunas
    df = df[original_cols]

    generate_data_quality_report(df)
    logger.info("=== TRANSFORMAÇÃO CONCLUÍDA ===")
    return df

def main():
    """Carrega dados brutos, aplica transformações e salva tratados."""
    try:
        parquet_path = RAW_DIR / "dados_api.parquet"
        csv_path = RAW_DIR / "dados_api.csv"

        if parquet_path.exists():
            logger.info(f"Lendo {parquet_path}")
            df = pd.read_parquet(parquet_path)
        elif csv_path.exists():
            logger.info(f"Lendo {csv_path}")
            df = pd.read_csv(csv_path, low_memory=False)
        else:
            raise FileNotFoundError("Nenhum arquivo encontrado em data/raw/")

        df_transformed = transform(df)

        processed_parquet = PROCESSED_DIR / "dados_tratados.parquet"
        processed_csv = PROCESSED_DIR / "dados_tratados.csv"

        df_transformed.to_parquet(processed_parquet, index=False, engine="pyarrow")
        df_transformed.to_csv(processed_csv, index=False, encoding="utf-8-sig")

        logger.info(f"✅ Dados salvos em:")
        logger.info(f" - {processed_parquet}")
        logger.info(f" - {processed_csv}")

    except Exception as e:
        logger.error(f"Erro durante a transformação: {e}")
        raise

if __name__ == "__main__":
    main()
