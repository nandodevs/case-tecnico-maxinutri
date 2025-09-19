# load.py
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from psycopg2.extras import execute_values
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

def load_and_convert_data(csv_path):
    """
    Carrega o CSV e converte todas as colunas para os tipos corretos.
    """
    logger.info(f"Carregando dados de: {csv_path}")

    df = pd.read_csv(
        csv_path,
        low_memory=False,
        parse_dates=[
            'order_purchase_timestamp', 'order_approved_at',
            'order_delivered_carrier_date', 'order_delivered_customer_date',
            'order_estimated_delivery_date', 'review_creation_date',
            'review_answer_timestamp'
        ],
        dtype={
            'order_id': str, 'customer_id': str, 'product_id': str, 'seller_id': str,
            'review_id': str, 'customer_zip_code_prefix': str,
            'seller_zip_code_prefix': str, 'geolocation_zip_code_prefix': str
        }
    )
    logger.info(f"Dados carregados: {len(df)} registros, {len(df.columns)} colunas")

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].replace({'nan': pd.NA, 'None': pd.NA})

    critical_columns = ['order_id', 'customer_id', 'product_id']
    existing_critical = [col for col in critical_columns if col in df.columns]

    if existing_critical:
        before_cleanup = len(df)
        df.dropna(subset=existing_critical, inplace=True)
        logger.info(f"Linhas removidas por falta de dados críticos: {before_cleanup - len(df)}")

    if 'order_id' in df.columns and 'product_id' in df.columns:
        key = ['order_id', 'product_id']
        before_dedup = len(df)
        df.drop_duplicates(subset=key, inplace=True)
        logger.info(f"Duplicatas removidas: {before_dedup - len(df)}")

    logger.info(f"Dados finais prontos para carga: {len(df)} registros")
    return df

def get_dim_keys_in_batch(cur, table_name, business_key_col, business_keys, surrogate_key_col="sk"):
    """Busca um lote de chaves substitutas de uma só vez."""
    if not business_keys:
        return {}
    query = sql.SQL("""
        SELECT {business_key_col}, {surrogate_key_col}
        FROM {table}
        WHERE {business_key_col} = ANY(%s);
    """).format(
        table=sql.Identifier(table_name),
        business_key_col=sql.Identifier(business_key_col),
        surrogate_key_col=sql.Identifier(f"{table_name.split('_')[1]}_{surrogate_key_col}")
    )
    try:
        cur.execute(query, (list(business_keys),))
        return dict(cur.fetchall())
    except psycopg2.Error as e:
        logger.error(f"Erro ao buscar chaves em lote para {table_name}: {e}")
        cur.connection.rollback()
        return {}

def insert_in_batch(cur, table_name, columns, data, conflict_target, operation_name):
    """
    Insere dados em lote usando execute_values para alta performance.
    """
    if not data:
        logger.info(f"Nenhum dado para inserir em {table_name}.")
        return 0
    query = sql.SQL("""
        INSERT INTO {table} ({columns})
        VALUES %s
        ON CONFLICT ({conflict_target}) DO NOTHING;
    """).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
        conflict_target=sql.SQL(', ').join(map(sql.Identifier, conflict_target))
    )
    try:
        execute_values(cur, query, data, page_size=500)
        logger.info(f"Sucesso na operação '{operation_name}': {cur.rowcount} linhas inseridas/afetadas.")
        return cur.rowcount
    except psycopg2.Error as e:
        logger.error(f"Erro na inserção em lote para '{operation_name}': {e}")
        cur.connection.rollback()
        return 0

def execute_schema(cur):
    """
    Executa o script de criação de schema de forma idempotente, ignorando erros de
    tabelas ou restrições que já existam.
    """
    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        for i, statement in enumerate(statements):
            try:
                if statement:
                    cur.execute(statement)
                    if i % 5 == 0:
                        logger.info(f"Executando statement {i+1}/{len(statements)}")
            except psycopg2.Error as e:
                if "already exists" in str(e) or "duplicate" in str(e) or "duplicate key" in str(e) or "restrição" in str(e):
                    logger.debug(f"Entidade já existe: {statement[:50]}...")
                    cur.connection.rollback()
                else:
                    logger.warning(f"Erro no statement {i+1}: {e}")
                    raise
        logger.info("Schema criado/atualizado com sucesso.")
    except FileNotFoundError:
        logger.error(f"Arquivo schema.sql não encontrado em: {SCHEMA_PATH}")
        raise
    except Exception as e:
        logger.error(f"Erro ao executar o schema: {e}")
        raise

def insert_dim_cliente(cur, df: pd.DataFrame):
    """Insere dados na tabela de dimensão de clientes."""
    logger.info("Inserindo dimensão de clientes...")
    required_cols = ["customer_id", "customer_city", "customer_state", "customer_zip_code_prefix"]
    if not all(col in df.columns for col in required_cols):
        logger.warning("Colunas de cliente não encontradas, pulando inserção.")
        return
    clientes = df[required_cols].drop_duplicates().dropna(subset=['customer_id'])
    data_to_insert = [tuple(row) for row in clientes.itertuples(index=False)]
    insert_in_batch(
        cur,
        table_name="dim_cliente",
        columns=["cliente_id", "cidade", "estado", "cep_prefix"],
        data=data_to_insert,
        conflict_target=["cliente_id"],
        operation_name="dim_cliente"
    )

def insert_dim_produto(cur, df: pd.DataFrame):
    """Insere dados na tabela de dimensão de produtos."""
    logger.info("Inserindo dimensão de produtos...")
    required_cols = ["product_id", "product_category_name", "product_weight_g",
                    "product_length_cm", "product_height_cm", "product_width_cm", "product_photos_qty"]
    existing_cols = [col for col in required_cols if col in df.columns]
    if not existing_cols:
        logger.warning("Colunas de produto não encontradas, pulando inserção.")
        return
    produtos_df = df[existing_cols].drop_duplicates().dropna(subset=['product_id']).copy()
    if 'product_photos_qty' in produtos_df.columns:
        produtos_df['fotos_qty'] = pd.to_numeric(produtos_df['product_photos_qty'], errors='coerce')
        produtos_df['fotos_qty'] = produtos_df['fotos_qty'].apply(lambda x: int(x) if pd.notna(x) else None)
    else:
        produtos_df['fotos_qty'] = None
    columns_for_insert = [
        "product_id", "product_category_name", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    produtos_df_insert = produtos_df[columns_for_insert].copy()
    produtos_df_insert['fotos_qty'] = produtos_df['fotos_qty']
    data_to_insert = [
        tuple(None if pd.isna(val) else val for val in row)
        for row in produtos_df_insert.itertuples(index=False)
    ]
    logger.info(f"Preparando para inserir {len(data_to_insert)} registros na dim_produto.")
    insert_in_batch(
        cur,
        table_name="dim_produto",
        columns=["produto_id", "categoria", "peso_g", "comprimento_cm", "altura_cm", "largura_cm", "fotos_qty"],
        data=data_to_insert,
        conflict_target=["produto_id"],
        operation_name="dim_produto"
    )

def insert_dim_tempo(cur, df: pd.DataFrame):
    """
    Insere dados na tabela de dimensão de tempo.
    Calcula trimestre e semana do ano.
    """
    logger.info("Inserindo dimensão de tempo...")
    date_col = None
    for col in ['order_purchase_timestamp', 'order_approved_at']:
        if col in df.columns:
            date_col = col
            break
    if not date_col:
        logger.warning("Coluna de data não encontrada, pulando inserção de tempo.")
        return
    df_temp = df.copy()
    df_temp['data_pedido'] = pd.to_datetime(df_temp[date_col], errors='coerce').dt.date
    datas = df_temp['data_pedido'].dropna().drop_duplicates()
    data_to_insert = [
        (data, data.year, data.month, data.day, data.strftime('%A'),
         pd.Timestamp(data).quarter, pd.Timestamp(data).isocalendar().week)
        for data in datas if pd.notna(data)
    ]
    insert_in_batch(
        cur,
        table_name="dim_tempo",
        columns=["data", "ano", "mes", "dia", "dia_da_semana", "trimestre", "semana_ano"],
        data=data_to_insert,
        conflict_target=["data"],
        operation_name="dim_tempo"
    )

def insert_dim_avaliacao(cur, df: pd.DataFrame):
    """
    Insere dados na tabela de dimensão de avaliação.
    Garante a inserção correta com base na review_id.
    """
    logger.info("Inserindo dimensão de avaliação...")
    review_cols = ["review_id", "review_score", "review_comment_title", "review_comment_message"]
    if 'review_id' not in df.columns:
        logger.warning("Coluna 'review_id' não encontrada. A dimensão de avaliação não será populada.")
        return
    df_reviews = df[df['review_id'].notna()].copy()
    if 'review_score' not in df_reviews.columns: df_reviews['review_score'] = None
    if 'review_comment_title' not in df_reviews.columns: df_reviews['review_comment_title'] = "Sem título"
    if 'review_comment_message' not in df_reviews.columns: df_reviews['review_comment_message'] = "Sem comentários"
    avaliacoes = df_reviews[review_cols].drop_duplicates(subset=['review_id'])
    data_to_insert = [tuple(row) for row in avaliacoes.itertuples(index=False)]
    logger.info(f"Preparando para inserir {len(data_to_insert)} registros na dim_avaliacao.")
    insert_in_batch(
        cur,
        table_name="dim_avaliacao",
        columns=["review_id", "review_score", "review_comment_title", "review_comment_message"],
        data=data_to_insert,
        conflict_target=["review_id"],
        operation_name="dim_avaliacao"
    )

def insert_fato_pedido(cur, df: pd.DataFrame):
    """Insere dados na tabela de fatos de pedidos com controle de transação."""
    logger.info("Inserindo fatos de pedidos...")
    inserted = 0
    skipped = 0
    batch_size = 1000
    for batch_start in range(0, len(df), batch_size):
        batch_end = min(batch_start + batch_size, len(df))
        batch_df = df.iloc[batch_start:batch_end].copy()
        logger.info(f"Processando lote de fatos: {batch_start}-{batch_end} de {len(df)}")
        batch_df.dropna(subset=['order_id', 'customer_id', 'product_id'], inplace=True)
        if 'review_id' not in batch_df.columns:
            logger.warning("Coluna 'review_id' não encontrada no lote. A coluna de fatos 'avaliacao_sk' será nula.")
            batch_df['review_id'] = None
        cliente_keys = get_dim_keys_in_batch(cur, 'dim_cliente', 'cliente_id', set(batch_df['customer_id'].dropna()))
        produto_keys = get_dim_keys_in_batch(cur, 'dim_produto', 'produto_id', set(batch_df['product_id'].dropna()))
        batch_df['data_pedido'] = pd.to_datetime(batch_df['order_purchase_timestamp'], errors='coerce').dt.date
        tempo_keys = get_dim_keys_in_batch(cur, 'dim_tempo', 'data', set(batch_df['data_pedido'].dropna()))
        if 'review_id' in batch_df.columns:
             avaliacao_keys = get_dim_keys_in_batch(cur, 'dim_avaliacao', 'review_id', set(batch_df['review_id'].dropna()), "avaliacao_sk")
        else:
             avaliacao_keys = {}
        batch_df['cliente_sk'] = batch_df['customer_id'].map(cliente_keys)
        batch_df['produto_sk'] = batch_df['product_id'].map(produto_keys)
        batch_df['tempo_sk'] = batch_df['data_pedido'].map(tempo_keys)
        batch_df['avaliacao_sk'] = batch_df['review_id'].map(avaliacao_keys) if 'review_id' in batch_df.columns else None
        batch_df.dropna(subset=['order_id', 'cliente_sk', 'produto_sk', 'tempo_sk'], inplace=True)
        if batch_df.empty:
            logger.warning("Nenhum dado válido no lote para inserir na tabela de fatos.")
            skipped += (batch_end - batch_start)
            continue
        batch_df.dropna(subset=['order_id'], inplace=True)
        columns_to_insert = [
            'pedido_id', 'cliente_sk', 'produto_sk', 'tempo_sk', 'avaliacao_sk',
            'status_pedido', 'preco', 'frete', 'data_aprovacao',
            'data_entrega_transportadora', 'data_entrega_cliente',
            'data_entrega_estimada', 'quantidade_itens'
        ]
        for col in columns_to_insert:
            if col not in batch_df.columns:
                batch_df[col] = None
        data_to_insert = [
            tuple(row) for row in batch_df[columns_to_insert].itertuples(index=False)
        ]
        batch_inserted = insert_in_batch(
            cur,
            table_name="fato_pedido",
            columns=columns_to_insert,
            data=data_to_insert,
            conflict_target=["pedido_id"],
            operation_name=f"fato_pedido_lote_{batch_start}"
        )
        cur.connection.commit()
        inserted += batch_inserted
        skipped += (len(batch_df) - batch_inserted)
    logger.info(f"TOTAL - Pedidos inseridos: {inserted}, Pulados: {skipped}")
def main(cur):
    """Função principal para o ETL com controle robusto de transações."""
    try:
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")
        df = load_and_convert_data(CSV_PATH)
        logger.info("=== CRIANDO SCHEMA ===")
        execute_schema(cur)
        cur.connection.commit()
        logger.info("=== INSERINDO DIMENSÕES ===")
        insert_dim_cliente(cur, df)
        cur.connection.commit()
        logger.info("Dimensão de clientes commitada.")
        insert_dim_produto(cur, df)
        cur.connection.commit()
        logger.info("Dimensão de produtos commitada.")
        insert_dim_tempo(cur, df)
        cur.connection.commit()
        logger.info("Dimensão de tempo commitada.")
        insert_dim_avaliacao(cur, df)
        cur.connection.commit()
        logger.info("Dimensão de avaliação commitada.")
        logger.info("=== INSERINDO FATOS ===")
        insert_fato_pedido(cur, df)
        logger.info("✅ ETL concluído com sucesso!")
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Erro de banco de dados: {e}")
        cur.connection.rollback()
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        if cur and cur.connection:
            cur.connection.rollback()
        raise
    finally:
        logger.info("Função de carga finalizada.")
if __name__ == "__main__":
    main()