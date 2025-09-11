import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from pathlib import Path

PROCESSED_DIR = Path(os.path.join(os.path.dirname(__file__), "..", "data", "processed")).resolve()
CSV_PATH = PROCESSED_DIR / "dados_tratados.csv"
SCHEMA_PATH = Path(os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")).resolve()

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "desafiodb",
    "user": "admin",
    "password": "admin@123"
}

def execute_schema(cur):
    """Executa o script de criação de schema."""
    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        for statement in schema_sql.split(';'):
            stmt = statement.strip()
            if stmt:
                cur.execute(stmt)
        print("Schema criado com sucesso.")
    except (psycopg2.OperationalError, FileNotFoundError) as e:
        print(f"Erro ao executar o schema: {e}")
        raise

def insert_dim_cliente(cur, df):
    """Insere dados na tabela de dimensão de clientes."""
    clientes = df[["customer_id", "customer_city", "customer_state", "customer_zip_code_prefix"]].drop_duplicates()
    for _, row in clientes.iterrows():
        cur.execute(
            """INSERT INTO dim_cliente (cliente_id, cidade, estado, cep_prefix)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cliente_id) DO NOTHING;""",
            (row["customer_id"], row["customer_city"], row["customer_state"], row["customer_zip_code_prefix"])
        )

def insert_dim_produto(cur, df):
    """Insere dados na tabela de dimensão de produtos."""
    produtos = df[["product_id", "product_category_name", "product_weight_g", "product_length_cm",
                    "product_height_cm", "product_width_cm", "product_photos_qty"]].drop_duplicates()
    for _, row in produtos.iterrows():
        cur.execute(
            """INSERT INTO dim_produto (produto_id, categoria, peso_g, comprimento_cm, altura_cm, largura_cm, fotos_qty)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (produto_id) DO NOTHING;""",
            (row["product_id"], row["product_category_name"], row["product_weight_g"], row["product_length_cm"],
             row["product_height_cm"], row["product_width_cm"], row["product_photos_qty"])
        )

def insert_dim_tempo(cur, df):
    """Insere dados na tabela de dimensão de tempo."""
    df['data_pedido'] = pd.to_datetime(df['order_purchase_timestamp']).dt.date
    datas = df['data_pedido'].drop_duplicates()
    for data in datas:
        cur.execute(
            """INSERT INTO dim_tempo (data, ano, mes, dia, dia_da_semana)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (data) DO NOTHING;""",
            (data, data.year, data.month, data.day, data.strftime('%A'))
        )

def insert_dim_avaliacao(cur, df):
    """Insere dados na tabela de dimensão de avaliação."""
    avaliacoes = df[["review_score", "review_comment_title", "review_comment_message"]].drop_duplicates()
    for _, row in avaliacoes.iterrows():
        cur.execute(
            """INSERT INTO dim_avaliacao (review_score, review_comment_title, review_comment_message)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;""",
            (row["review_score"], row["review_comment_title"], row["review_comment_message"])
        )
def get_dim_key(cur, table_name, business_key_col, business_key_value, surrogate_key_col):
    """Função genérica para buscar a chave substituta de uma dimensão."""
    cur.execute(sql.SQL("SELECT {} FROM {} WHERE {} = %s;").format(
        sql.Identifier(surrogate_key_col),
        sql.Identifier(table_name),
        sql.Identifier(business_key_col)
    ), (business_key_value,))
    result = cur.fetchone()
    return result[0] if result else None

def insert_fato_pedido(cur, df):
    """Insere dados na tabela de fatos de pedidos."""
    for _, row in df.iterrows():
        try:
            # Obter chaves substitutas das dimensões
            cliente_sk = get_dim_key(cur, 'dim_cliente', 'cliente_id', row['customer_id'], 'cliente_sk')
            produto_sk = get_dim_key(cur, 'dim_produto', 'produto_id', row['product_id'], 'produto_sk')
            
            data_pedido = pd.to_datetime(row['order_purchase_timestamp']).date()
            tempo_sk = get_dim_key(cur, 'dim_tempo', 'data', data_pedido, 'tempo_sk')

            avaliacao_sk = get_dim_key(cur, 'dim_avaliacao', 'review_score', row['review_score'], 'avaliacao_sk')
            
            # Inserir na tabela de fatos
            cur.execute(
                """INSERT INTO fato_pedido (
                    pedido_id, cliente_sk, produto_sk, tempo_sk, avaliacao_sk,
                    status_pedido, preco, frete
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pedido_id) DO NOTHING;""",
                (
                    row["order_id"], cliente_sk, produto_sk, tempo_sk, avaliacao_sk,
                    row["order_status"], row["price"], row["freight_value"]
                )
            )
        except Exception as e:
            print(f"Erro ao inserir o pedido {row['order_id']}: {e}")

def main():
    """Função principal para o ETL."""
    try:
        df = pd.read_csv(CSV_PATH)
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        execute_schema(cur)
        conn.commit()
        
        insert_dim_cliente(cur, df)
        insert_dim_produto(cur, df)
        insert_dim_tempo(cur, df)
        insert_dim_avaliacao(cur, df)
        conn.commit()
        
        insert_fato_pedido(cur, df)
        conn.commit()

        print("Dados carregados no modelo estrela com sucesso!")

    except (psycopg2.Error, pd.errors.EmptyDataError) as e:
        print(f"Erro: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()