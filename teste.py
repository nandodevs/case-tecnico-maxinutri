import psycopg2

# Altere estes dados conforme sua configuração
HOST = "localhost"      # Use 'postgres' se rodando de dentro do container Airflow, 'localhost' se rodando do seu computador
PORT = 5433         # Porta interna do container (normalmente 5432)
USER = "postgres"
PASSWORD = "admin@0722"
DBNAME = "desafio_db"

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            user=USER,
            password=PASSWORD
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("Conexão bem-sucedida! Versão do PostgreSQL:", version)
        cur.close()
        conn.close()
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)

if __name__ == "__main__":
    test_postgres_connection()