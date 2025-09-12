import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import numpy as np
from pathlib import Path
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.sql import SQL, Identifier

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "processed")).resolve()
CSV_PATH = PROCESSED_DIR / "dados_tratados.csv"
SCHEMA_PATH = Path(os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")).resolve()

def create_database_if_not_exists():
    """
    Cria o banco de dados de destino se ele não existir.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default", database="desafiodb")
    conn = None
    cur = None
    try:
        conn = hook.get_conn()
        conn.autocommit = True
        cur = conn.cursor()

        db_name = "desafiodb"

        # Verifica se o banco de dados já existe de forma segura
        cur.execute(SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (db_name,))
        exists = cur.fetchone()

        if not exists:
            logger.info(f"Criando o banco de dados '{db_name}'...")
            # Cria o banco de forma segura
            cur.execute(SQL("CREATE DATABASE {}").format(Identifier(db_name)))
            logger.info(f"Banco de dados '{db_name}' criado com sucesso.")
        else:
            logger.info(f"Banco de dados '{db_name}' já existe.")

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_and_convert_data(csv_path):
    """
    Carrega o CSV e converte todas as colunas para os tipos corretos.
    """
    logger.info(f"Carregando dados de: {csv_path}")
    
    # Carregar CSV com configurações otimizadas
    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"Dados carregados: {len(df)} registros, {len(df.columns)} colunas")
    
    # ========== CONVERSÕES DE TIPO ==========
    
    # 1. IDs como string (preservar formato)
    id_columns = ['order_id', 'customer_id', 'product_id', 'seller_id', 'review_id']
    for col in id_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', np.nan)
    
    # 2. CEPs como string (preservar zeros à esquerda)
    cep_columns = ['customer_zip_code_prefix', 'seller_zip_code_prefix', 'geolocation_zip_code_prefix']
    for col in cep_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('.0', '').replace('nan', np.nan)
            # Limitar tamanho para evitar valores muito grandes
            df[col] = df[col].str[:10]
    
    # 3. Cidades e textos
    text_columns = ['customer_city', 'customer_state', 'seller_city', 'seller_state', 
                   'product_category_name', 'review_comment_title', 'review_comment_message']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', np.nan)
            if col.endswith('_state'):
                df[col] = df[col].str.upper().str.strip()
            elif 'city' in col:
                df[col] = df[col].str.title().str.strip()
    
    # 4. Valores numéricos com validação
    numeric_conversions = {
        'product_weight_g': {'type': 'float', 'min': 0, 'max': 50000},
        'product_length_cm': {'type': 'float', 'min': 0, 'max': 500},
        'product_height_cm': {'type': 'float', 'min': 0, 'max': 500},
        'product_width_cm': {'type': 'float', 'min': 0, 'max': 500},
        'product_photos_qty': {'type': 'int', 'min': 0, 'max': 50},
        'price': {'type': 'float', 'min': 0, 'max': 100000},
        'freight_value': {'type': 'float', 'min': 0, 'max': 10000},
        'review_score': {'type': 'int', 'min': 1, 'max': 5},
        'payment_installments': {'type': 'int', 'min': 1, 'max': 24},
        'payment_value': {'type': 'float', 'min': 0, 'max': 100000}
    }
    
    for col, config in numeric_conversions.items():
        if col in df.columns:
            # Converter para numérico
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Aplicar validação de range
            min_val, max_val = config['min'], config['max']
            df[col] = df[col].where((df[col] >= min_val) & (df[col] <= max_val), np.nan)
            
            logger.info(f"Coluna {col}: {df[col].notna().sum()} valores válidos")
    
    # 5. Datas
    date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
                   'order_delivered_customer_date', 'order_estimated_delivery_date', 
                   'review_creation_date', 'review_answer_timestamp']
    
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Validar range de anos
            df[col] = df[col].where(df[col].dt.year.between(2000, 2030), np.nan)
    
    # 6. Limpeza final - remover linhas sem dados críticos
    critical_columns = ['order_id', 'customer_id', 'product_id']
    existing_critical = [col for col in critical_columns if col in df.columns]
    
    if existing_critical:
        before_cleanup = len(df)
        df = df.dropna(subset=existing_critical)
        logger.info(f"Linhas removidas por falta de dados críticos: {before_cleanup - len(df)}")
    
    # Remover duplicatas
    if 'order_id' in df.columns and 'product_id' in df.columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['order_id', 'product_id'])
        logger.info(f"Duplicatas removidas: {before_dedup - len(df)}")
    
    logger.info(f"Dados finais após conversão: {len(df)} registros")
    return df

def execute_schema(cur):
    """Executa o script de criação de schema com tratamento de erro robusto."""
    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        
        # Executar cada statement separadamente com tratamento de erro individual
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements):
            try:
                cur.execute(statement)
                logger.info(f"Statement {i+1}/{len(statements)} executado com sucesso")
            except psycopg2.Error as e:
                logger.warning(f"Erro no statement {i+1}: {e}")
                # Se houver erro, fazer rollback e tentar continuar
                cur.connection.rollback()
                raise
        
        logger.info("Schema criado/atualizado com sucesso.")
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo schema.sql não encontrado: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Erro ao executar o schema: {e}")
        raise

def safe_insert_value(value, expected_type='string'):
    """
    Converte valores de forma segura para inserção no banco.
    """
    if pd.isna(value) or value is None or str(value).lower() in ['nan', 'none', '']:
        return None
    
    try:
        if expected_type == 'int':
            return int(float(value))  # Converter via float primeiro para casos como "5.0"
        elif expected_type == 'float':
            return float(value)
        else:
            return str(value).strip()
    except (ValueError, OverflowError):
        return None

def execute_with_retry(cur, query, params, operation_name, max_retries=3):
    """
    Executa uma query com retry em caso de erro de transação.
    """
    for attempt in range(max_retries):
        try:
            cur.execute(query, params)
            return True
        except psycopg2.Error as e:
            logger.warning(f"Erro na tentativa {attempt + 1} de {operation_name}: {e}")
            
            # Se a transação foi abortada, fazer rollback e tentar novamente
            if "current transaction is aborted" in str(e):
                cur.connection.rollback()
                logger.info(f"Rollback executado, tentando novamente...")
            
            if attempt == max_retries - 1:
                logger.error(f"Falha após {max_retries} tentativas em {operation_name}")
                return False
    
    return False

def insert_dim_cliente(cur, df):
    """Insere dados na tabela de dimensão de clientes."""
    logger.info("Inserindo dimensão de clientes...")
    
    required_cols = ["customer_id", "customer_city", "customer_state", "customer_zip_code_prefix"]
    if not all(col in df.columns for col in required_cols):
        logger.warning("Colunas de cliente não encontradas, pulando inserção")
        return
    
    clientes = df[required_cols].drop_duplicates()
    inserted = 0
    failed = 0
    
    for _, row in clientes.iterrows():
        success = execute_with_retry(
            cur,
            """INSERT INTO dim_cliente (cliente_id, cidade, estado, cep_prefix)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cliente_id) DO NOTHING;""",
            (
                safe_insert_value(row["customer_id"]),
                safe_insert_value(row["customer_city"]),
                safe_insert_value(row["customer_state"]),
                safe_insert_value(row["customer_zip_code_prefix"])
            ),
            f"inserção cliente {row['customer_id']}"
        )
        
        if success:
            inserted += 1
        else:
            failed += 1
    
    logger.info(f"Clientes - Inseridos: {inserted}, Falhas: {failed}")

def insert_dim_produto(cur, df):
    """Insere dados na tabela de dimensão de produtos."""
    logger.info("Inserindo dimensão de produtos...")
    
    required_cols = ["product_id", "product_category_name", "product_weight_g", 
                    "product_length_cm", "product_height_cm", "product_width_cm", "product_photos_qty"]
    
    existing_cols = [col for col in required_cols if col in df.columns]
    if not existing_cols:
        logger.warning("Colunas de produto não encontradas, pulando inserção")
        return
    
    produtos = df[existing_cols].drop_duplicates()
    inserted = 0
    failed = 0
    
    for _, row in produtos.iterrows():
        success = execute_with_retry(
            cur,
            """INSERT INTO dim_produto (produto_id, categoria, peso_g, comprimento_cm, altura_cm, largura_cm, fotos_qty)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (produto_id) DO NOTHING;""",
            (
                safe_insert_value(row.get("product_id")),
                safe_insert_value(row.get("product_category_name")),
                safe_insert_value(row.get("product_weight_g"), 'float'),
                safe_insert_value(row.get("product_length_cm"), 'float'),
                safe_insert_value(row.get("product_height_cm"), 'float'),
                safe_insert_value(row.get("product_width_cm"), 'float'),
                safe_insert_value(row.get("product_photos_qty"), 'int')
            ),
            f"inserção produto {row.get('product_id')}"
        )
        
        if success:
            inserted += 1
        else:
            failed += 1
    
    logger.info(f"Produtos - Inseridos: {inserted}, Falhas: {failed}")

def insert_dim_tempo(cur, df):
    """Insere dados na tabela de dimensão de tempo."""
    logger.info("Inserindo dimensão de tempo...")
    
    date_col = None
    for col in ['order_purchase_timestamp', 'order_approved_at']:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        logger.warning("Coluna de data não encontrada, pulando inserção de tempo")
        return
    
    df_temp = df.copy()
    df_temp['data_pedido'] = pd.to_datetime(df_temp[date_col], errors='coerce').dt.date
    datas = df_temp['data_pedido'].dropna().drop_duplicates()
    
    inserted = 0
    failed = 0
    
    for data in datas:
        if pd.notna(data):
            success = execute_with_retry(
                cur,
                """INSERT INTO dim_tempo (data, ano, mes, dia, dia_da_semana)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (data) DO NOTHING;""",
                (data, data.year, data.month, data.day, data.strftime('%A')),
                f"inserção data {data}"
            )
            
            if success:
                inserted += 1
            else:
                failed += 1
    
    logger.info(f"Datas - Inseridas: {inserted}, Falhas: {failed}")

def insert_dim_avaliacao(cur, df):
    """Insere dados na tabela de dimensão de avaliação."""
    logger.info("Inserindo dimensão de avaliação...")
    
    review_cols = ["review_score", "review_comment_title", "review_comment_message"]
    existing_review_cols = [col for col in review_cols if col in df.columns]
    
    if not existing_review_cols:
        logger.warning("Colunas de avaliação não encontradas, pulando inserção")
        return
    
    # Filtrar apenas linhas que têm review_score válido
    df_reviews = df[df['review_score'].notna()].copy() if 'review_score' in df.columns else df.copy()
    
    if len(df_reviews) == 0:
        logger.warning("Nenhuma avaliação válida encontrada")
        return
    
    avaliacoes = df_reviews[existing_review_cols].drop_duplicates()
    inserted = 0
    failed = 0
    
    for _, row in avaliacoes.iterrows():
        success = execute_with_retry(
            cur,
            """INSERT INTO dim_avaliacao (review_score, review_comment_title, review_comment_message)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;""",
            (
                safe_insert_value(row.get("review_score"), 'int'),
                safe_insert_value(row.get("review_comment_title", "Sem título")),
                safe_insert_value(row.get("review_comment_message", "Sem comentários"))
            ),
            f"inserção avaliação"
        )
        
        if success:
            inserted += 1
        else:
            failed += 1
    
    logger.info(f"Avaliações - Inseridas: {inserted}, Falhas: {failed}")

def get_dim_key_safe(cur, table_name, business_key_col, business_key_value, surrogate_key_col):
    """Função segura para buscar chave substituta com tratamento de transação abortada."""
    if business_key_value is None or pd.isna(business_key_value):
        return None
    
    try:
        cur.execute(sql.SQL("SELECT {} FROM {} WHERE {} = %s LIMIT 1;").format(
            sql.Identifier(surrogate_key_col),
            sql.Identifier(table_name),
            sql.Identifier(business_key_col)
        ), (business_key_value,))
        
        result = cur.fetchone()
        return result[0] if result else None
        
    except psycopg2.Error as e:
        if "current transaction is aborted" in str(e):
            logger.warning(f"Transação abortada ao buscar chave {table_name}, fazendo rollback")
            cur.connection.rollback()
        return None

def insert_fato_pedido(cur, df):
    """Insere dados na tabela de fatos de pedidos com controle de transação."""
    logger.info("Inserindo fatos de pedidos...")
    
    inserted = 0
    skipped = 0
    batch_size = 100
    
    # Processar em lotes para melhor controle de transação
    for batch_start in range(0, len(df), batch_size):
        batch_end = min(batch_start + batch_size, len(df))
        batch_df = df.iloc[batch_start:batch_end]
        
        logger.info(f"Processando lote {batch_start}-{batch_end} de {len(df)}")
        
        batch_inserted = 0
        batch_skipped = 0
        
        for idx, row in batch_df.iterrows():
            try:
                # Obter chaves substitutas das dimensões
                cliente_sk = get_dim_key_safe(cur, 'dim_cliente', 'cliente_id', 
                                            safe_insert_value(row.get('customer_id')), 'cliente_sk')
                produto_sk = get_dim_key_safe(cur, 'dim_produto', 'produto_id', 
                                            safe_insert_value(row.get('product_id')), 'produto_sk')
                
                # Data do pedido
                data_pedido = None
                for date_col in ['order_purchase_timestamp', 'order_approved_at']:
                    if date_col in df.columns and pd.notna(row.get(date_col)):
                        data_pedido = pd.to_datetime(row[date_col], errors='coerce').date()
                        break
                
                tempo_sk = get_dim_key_safe(cur, 'dim_tempo', 'data', data_pedido, 'tempo_sk') if data_pedido else None
                
                # Avaliação
                avaliacao_sk = None
                if 'review_score' in df.columns and pd.notna(row.get('review_score')):
                    review_score = safe_insert_value(row['review_score'], 'int')
                    if review_score:
                        avaliacao_sk = get_dim_key_safe(cur, 'dim_avaliacao', 'review_score', 
                                                      review_score, 'avaliacao_sk')
                
                # Verificar se temos dados mínimos necessários
                order_id = safe_insert_value(row.get('order_id'))
                if not order_id or not cliente_sk or not produto_sk:
                    batch_skipped += 1
                    continue
                
                # Inserir na tabela de fatos
                success = execute_with_retry(
                    cur,
                    """INSERT INTO fato_pedido (
                        pedido_id, cliente_sk, produto_sk, tempo_sk, avaliacao_sk,
                        status_pedido, preco, frete
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pedido_id) DO NOTHING;""",
                    (
                        order_id,
                        cliente_sk,
                        produto_sk,
                        tempo_sk,
                        avaliacao_sk,
                        safe_insert_value(row.get("order_status")),
                        safe_insert_value(row.get("price"), 'float'),
                        safe_insert_value(row.get("freight_value"), 'float')
                    ),
                    f"inserção pedido {order_id}"
                )
                
                if success:
                    batch_inserted += 1
                else:
                    batch_skipped += 1
                    
            except Exception as e:
                logger.warning(f"Erro não tratado no pedido {row.get('order_id')}: {e}")
                batch_skipped += 1
        
        # Commit do lote
        try:
            cur.connection.commit()
            logger.info(f"Lote commitado - Inseridos: {batch_inserted}, Pulados: {batch_skipped}")
        except psycopg2.Error as e:
            logger.error(f"Erro ao fazer commit do lote: {e}")
            cur.connection.rollback()
        
        inserted += batch_inserted
        skipped += batch_skipped
    
    logger.info(f"TOTAL - Pedidos inseridos: {inserted}, Pulados: {skipped}")

# Seu novo main em load.py
def main(cur):
    """Função principal para o ETL com controle robusto de transações."""
    try:
        # 1. Carregar e converter dados
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")
        
        df = load_and_convert_data(CSV_PATH)
        
        # O cur já é um cursor válido fornecido pela DAG
        
        # 2. Criar schema
        logger.info("=== CRIANDO SCHEMA ===")
        execute_schema(cur)
        cur.connection.commit()
        
        # 3. Inserir dimensões (cada uma em sua própria transação)
        logger.info("=== INSERINDO DIMENSÕES ===")
        
        insert_dim_cliente(cur, df)
        cur.connection.commit()
        logger.info("Dimensão clientes commitada")
        
        insert_dim_produto(cur, df)
        cur.connection.commit()
        logger.info("Dimensão produtos commitada")
        
        insert_dim_tempo(cur, df)
        cur.connection.commit()
        logger.info("Dimensão tempo commitada")
        
        insert_dim_avaliacao(cur, df)
        cur.connection.commit()
        logger.info("Dimensão avaliação commitada")
        
        # 4. Inserir fatos (processamento em lotes)
        logger.info("=== INSERINDO FATOS ===")
        insert_fato_pedido(cur, df)
        
        logger.info("✅ ETL concluído com sucesso!")
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Erro de banco de dados: {e}")
        cur.connection.rollback()
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        cur.connection.rollback()
    finally:
        # A conexão será fechada pela DAG
        pass

if __name__ == "__main__":
    main()