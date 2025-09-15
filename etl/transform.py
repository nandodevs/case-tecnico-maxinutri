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
from typing import Dict, Any

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "raw")).resolve()
PROCESSED_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "processed")).resolve()
PROCESSED_DIR.mkdir(exist_ok=True)

# Mapeamento de tipos de dados otimizados para cada coluna
DTYPE_MAPPING = {
    # IDs - mantidos como string para preservar formato
    'order_id': 'string',
    'customer_id': 'string', 
    'product_id': 'string',
    'seller_id': 'string',
    
    # Informações do cliente
    'customer_unique_id': 'string',
    'customer_zip_code_prefix': 'string',  # CEP como string
    'customer_city': 'string',
    'customer_state': 'category',  # Estados são categóricos
    
    # Informações do produto
    'product_category_name': 'category',
    'product_name_lenght': 'Int16',  # Nullable integer pequeno
    'product_description_lenght': 'Int32',
    'product_photos_qty': 'Int8',    # Poucos valores, int pequeno
    'product_weight_g': 'float32',   # Peso em gramas
    'product_length_cm': 'float32',  # Dimensões em cm
    'product_height_cm': 'float32',
    'product_width_cm': 'float32',
    
    # Informações do pedido
    'order_item_id': 'Int8',
    'order_status': 'category',
    'price': 'float32',              # Preço
    'freight_value': 'float32',      # Valor do frete
    
    # Vendedor
    'seller_zip_code_prefix': 'string',
    'seller_city': 'string', 
    'seller_state': 'category',
    
    # Pagamento
    'payment_sequential': 'Int8',
    'payment_type': 'category',
    'payment_installments': 'Int8',
    'payment_value': 'float32',
    
    # Review/Avaliação
    'review_id': 'string',
    'review_score': 'Int8',          # Score de 1-5
    'review_comment_title': 'string',
    'review_comment_message': 'string',
    
    # Geolocalização
    'geolocation_zip_code_prefix': 'string',
    'geolocation_lat': 'float32',
    'geolocation_lng': 'float32',
    'geolocation_city': 'string',
    'geolocation_state': 'category'
}

# Colunas que devem ser convertidas para datetime
DATE_COLUMNS = [
    'order_purchase_timestamp',
    'order_approved_at', 
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date',
    'shipping_limit_date',
    'review_creation_date',
    'review_answer_timestamp'
]

def detect_and_clean_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas completamente vazias e linhas com dados inválidos críticos.
    """
    initial_rows = len(df)
    logger.info(f"Linhas iniciais: {initial_rows}")
    
    # 1. Remover linhas completamente vazias
    df = df.dropna(how='all')
    logger.info(f"Após remover linhas vazias: {len(df)} linhas")
    
    # 2. Remover linhas onde IDs críticos são nulos/vazios
    critical_id_columns = ['order_id', 'customer_id', 'product_id']
    existing_critical = [col for col in critical_id_columns if col in df.columns]
    
    if existing_critical:
        # Remove linhas onde qualquer ID crítico é nulo
        mask = df[existing_critical].notna().all(axis=1)
        df = df[mask]
        logger.info(f"Após remover linhas sem IDs críticos: {len(df)} linhas")
        
        # Remove linhas onde IDs são strings vazias após conversão
        for col in existing_critical:
            if col in df.columns:
                mask = df[col].astype(str).str.strip().str.len() > 0
                df = df[mask]
    
    # 3. Remover linhas duplicadas baseadas em chaves de negócio
    if 'order_id' in df.columns and 'product_id' in df.columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['order_id', 'product_id'])
        logger.info(f"Duplicatas removidas: {before_dedup - len(df)}")
    
    logger.info(f"Total de linhas removidas: {initial_rows - len(df)}")
    return df

def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e normaliza colunas de texto.
    """
    text_columns = ['customer_city', 'seller_city', 'geolocation_city', 
                   'product_category_name', 'review_comment_title', 'review_comment_message']
    
    for col in text_columns:
        if col in df.columns:
            # Converter para string e limpar
            df[col] = df[col].astype(str)
            df[col] = df[col].str.strip()
            
            # Substituir valores inválidos por NaN
            df[col] = df[col].replace(['nan', 'NaN', 'none', 'None', '', 'null'], np.nan)
            
            # Normalização específica por tipo
            if 'city' in col:
                df[col] = df[col].str.title()  # Primeira letra maiúscula
            elif col == 'product_category_name':
                df[col] = df[col].str.lower()
    
    return df

def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e valida colunas numéricas antes da conversão de tipos.
    """
    numeric_columns = {
        'price': (0, 100000),           # Preço entre 0 e 100k
        'freight_value': (0, 10000),    # Frete entre 0 e 10k  
        'payment_value': (0, 100000),   # Pagamento entre 0 e 100k
        'product_weight_g': (0, 50000), # Peso até 50kg
        'product_length_cm': (0, 200),  # Dimensões até 2m
        'product_height_cm': (0, 200),
        'product_width_cm': (0, 200),
        'product_photos_qty': (0, 50),  # Até 50 fotos
        'review_score': (1, 5),         # Score de 1 a 5
        'payment_installments': (1, 24) # Até 24 parcelas
    }
    
    for col, (min_val, max_val) in numeric_columns.items():
        if col in df.columns:
            # Converter para numérico, colocando erros como NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Aplicar limites lógicos
            df[col] = df[col].where(
                (df[col] >= min_val) & (df[col] <= max_val), 
                np.nan
            )
    
    return df

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas de data para datetime.
    """
    for col in DATE_COLUMNS:
        if col in df.columns:
            # Converter para datetime
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Validar range de datas (e.g., entre 2000 e 2030)
            df[col] = df[col].where(
                (df[col].dt.year >= 2000) & (df[col].dt.year <= 2030),
                np.nan
            )
    
    return df

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica os tipos de dados otimizados para cada coluna.
    """
    logger.info("Iniciando conversão de tipos de dados...")
    
    for col in df.columns:
        if col in DTYPE_MAPPING:
            target_dtype = DTYPE_MAPPING[col]
            
            try:
                if target_dtype == 'category':
                    # Para categóricas, primeiro limpar e depois converter
                    df[col] = df[col].astype('string').astype('category')
                    
                elif target_dtype == 'string':
                    df[col] = df[col].astype('string')
                    
                elif target_dtype.startswith('Int'):  # Nullable integers
                    df[col] = df[col].astype(target_dtype)
                    
                elif target_dtype.startswith('float'):
                    df[col] = df[col].astype(target_dtype)
                    
                logger.info(f"Coluna '{col}' convertida para {target_dtype}")
                
            except Exception as e:
                logger.warning(f"Erro ao converter coluna '{col}' para {target_dtype}: {e}")
                # Manter tipo original em caso de erro
    
    return df

def generate_data_quality_report(df: pd.DataFrame) -> None:
    """
    Gera relatório de qualidade dos dados.
    """
    logger.info("=== RELATÓRIO DE QUALIDADE DOS DADOS ===")
    logger.info(f"Total de registros: {len(df):,}")
    logger.info(f"Total de colunas: {len(df.columns)}")
    
    # Informações sobre tipos de dados
    logger.info("\n--- TIPOS DE DADOS ---")
    dtype_counts = df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        logger.info(f"{dtype}: {count} colunas")
    
    # Informações sobre valores nulos
    logger.info("\n--- VALORES NULOS (Top 10) ---")
    null_counts = df.isnull().sum().sort_values(ascending=False).head(10)
    for col, null_count in null_counts.items():
        if null_count > 0:
            percentage = (null_count / len(df)) * 100
            logger.info(f"{col}: {null_count:,} ({percentage:.1f}%)")
    
    # Uso de memória
    memory_usage = df.memory_usage(deep=True).sum() / 1024**2  # MB
    logger.info(f"\nUso de memória: {memory_usage:.1f} MB")

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função principal de transformação com pipeline completo.
    """
    logger.info(f"=== INICIANDO TRANSFORMAÇÃO ===")
    logger.info(f"Dados de entrada: {len(df)} registros, {len(df.columns)} colunas")
    
    # Pipeline de transformação
    df = detect_and_clean_empty_rows(df)
    df = clean_text_columns(df)
    df = clean_numeric_columns(df)
    df = convert_date_columns(df)
    df = optimize_dtypes(df)
    
    # Gerar relatório de qualidade
    generate_data_quality_report(df)
    
    logger.info("=== TRANSFORMAÇÃO CONCLUÍDA ===")
    return df

def main():
    """
    Função principal que executa todo o processo de transformação.
    """
    try:
        # Definir caminhos dos arquivos
        parquet_path = RAW_DIR / "dados_api.parquet"
        csv_path = RAW_DIR / "dados_api.csv"

        # Carregar dados
        if parquet_path.exists():
            logger.info(f"Carregando dados de: {parquet_path}")
            df = pd.read_parquet(parquet_path)
        elif csv_path.exists():
            logger.info(f"Carregando dados de: {csv_path}")
            df = pd.read_csv(csv_path, low_memory=False)
        else:
            raise FileNotFoundError("Arquivo de dados brutos não encontrado em data/raw/")

        # Aplicar transformações
        df_transformed = transform(df)

        # Salvar dados transformados
        processed_parquet = PROCESSED_DIR / "dados_tratados.parquet"
        processed_csv = PROCESSED_DIR / "dados_tratados.csv"
        
        # Salvar em formato otimizado (Parquet preserva tipos de dados)
        df_transformed.to_parquet(processed_parquet, index=False, engine='pyarrow')
        
        # Salvar em CSV para compatibilidade
        df_transformed.to_csv(processed_csv, index=False, encoding='utf-8-sig')
        
        logger.info(f"✅ Dados salvos com sucesso:")
        logger.info(f"  - Parquet: {processed_parquet}")
        logger.info(f"  - CSV: {processed_csv}")
        
        # Comparação de tamanho de arquivos
        if processed_parquet.exists() and processed_csv.exists():
            parquet_size = processed_parquet.stat().st_size / 1024**2
            csv_size = processed_csv.stat().st_size / 1024**2
            logger.info(f"  - Tamanho Parquet: {parquet_size:.1f} MB")
            logger.info(f"  - Tamanho CSV: {csv_size:.1f} MB")
            logger.info(f"  - Economia: {((csv_size - parquet_size) / csv_size) * 100:.1f}%")

    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Arquivo de dados vazio: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante a transformação: {e}")
        raise

if __name__ == "__main__":
    main()