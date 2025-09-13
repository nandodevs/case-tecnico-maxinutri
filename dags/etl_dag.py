# Em dags/etl_dag.py
from __future__ import annotations

import pendulum
import sys
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.sql import SQL, Identifier
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Adiciona o diretório `etl` ao PYTHONPATH para que os scripts possam ser importados
sys.path.append(str(Path(__file__).parent.parent / "etl"))

# Importa as funções principais dos seus scripts
from extract import main as extract_main
from transform import main as transform_main
from load import main as load_main

with DAG(
    dag_id="desafio_etl_maxinutri",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl", "desafio"],
) as dag:
    
    def run_extract_task():
        """Executa a extração dos dados."""
        extract_main()

    def run_transform_task():
        """Executa a transformação dos dados."""
        transform_main()

    def run_create_database():
        """Cria o banco de dados se ele não existir."""
        # Use a nova conexão dedicada
        hook = PostgresHook(postgres_conn_id="meu_postgres")
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            db_name = "desafio_db"

            cur.execute(SQL("SELECT * FROM pg_database WHERE datname = %s"), (db_name,))
            exists = cur.fetchone()

            if not exists:
                logger.info(f"Criando o banco de dados '{db_name}'...")
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
    
    def run_apply_indexes():
        """Aplica os índices no banco de dados."""
        hook = PostgresHook(postgres_conn_id="meu_postgres", schema="desafio_db")
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            conn.autocommit = True
            cur = conn.cursor()

            indexes_path = Path(__file__).parent.parent / "sql" / "indexes.sql"
            with open(indexes_path, "r", encoding="utf-8") as f:
                sql_script = f.read()

            # Executa múltiplos statements separados por ";"
            statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
            for stmt in statements:
                cur.execute(stmt)

            logger.info("✅ Índices aplicados com sucesso.")

        except Exception as e:
            logger.error(f"Erro ao aplicar índices: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    
    def run_load_task():
        """Cria a conexão e executa o carregamento dos dados no banco de dados."""
        conn = None
        cur = None
        try:
            hook = PostgresHook(postgres_conn_id="meu_postgres")
            conn = hook.get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            load_main(cur=cur)
            
        except Exception as e:
            logger.error(f"Erro na tarefa de carga: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    extract_task = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=run_extract_task
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform_task
    )

    create_db_task = PythonOperator(
        task_id="create_database_if_not_exists",
        python_callable=run_create_database,
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=run_load_task,
    )
    
    apply_indexes_task = PythonOperator(
    task_id="apply_indexes",
    python_callable=run_apply_indexes,
    )

    extract_task >> transform_task >> create_db_task >> load_task >> apply_indexes_task
