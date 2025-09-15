# dags/etl_dag.py
from __future__ import annotations

import pendulum
import sys
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.sql import SQL, Identifier
from pathlib import Path
import logging
from psycopg2 import OperationalError, ProgrammingError, DataError, InterfaceError
from airflow.utils.state import State
from typing import Optional, List

# Adiciona o diretório `etl` ao PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent / "etl"))

# Importa as funções principais dos seus scripts
from etl.extract import main as extract_main
from etl.transform import main as transform_main
from etl.load import main as load_main

# Import do sistema de alertas
try:
    from etl.monitoring import AlertSystem
except ImportError as e:
    logging.warning(f"Sistema de alertas não disponível: {e}")
    # Fallback simples
    class AlertSystem:
        def __init__(self):
            pass
        def send_email_alert(self, *args, **kwargs):
            logging.warning("AlertSystem não inicializado. Email de alerta não enviado.")

# Cria uma instância de fallback, caso o módulo monitoring falhe
alert_system_fallback = AlertSystem()

logger = logging.getLogger(__name__)

# ---
# Funções de Callback

def on_failure_callback(context):
    """Callback para falhas de tarefas do Airflow."""
    try:
        alert_system = AlertSystem()
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        
        # Correção aqui: usa .get() para evitar erro se a chave não existir
        execution_date = context.get('execution_date', pendulum.now())
        
        exception = context.get('exception', 'Erro desconhecido')
        
        subject = f"Falha na DAG {dag_id} - Tarefa {task_id}"
        error_message = str(exception)
        
        is_critical = any(keyword in error_message.lower() for keyword in ['connection', 'database', 'timeout', 'critical', 'urgent'])
        
        simple_message = f"""Falha no pipeline ETL:
DAG: {dag_id}
Tarefa: {task_id}
Data: {execution_date}
Severidade: {'CRÍTICA' if is_critical else 'Normal'}
Erro: {error_message}
Acesse o Airflow para mais detalhes."""
        
        html_content = alert_system.create_html_alert(dag_id, task_id, error_message, execution_date, is_critical)
        
        success = alert_system.send_email_alert(subject, simple_message, html_content=html_content)
        
        if success:
            logger.info(f"✅ Alerta de falha enviado para {dag_id}.{task_id}")
        else:
            logger.warning(f"⚠️ Falha ao enviar alerta de email para {dag_id}.{task_id}")
            
    except Exception as e:
        logger.error(f"❌ Erro no sistema de alertas: {e}")

def on_success_callback(context):
    """Callback para sucesso de tarefas do Airflow."""
    try:
        alert_system = AlertSystem()
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        
        # Correção aqui: usa .get() para evitar erro se a chave não existir
        execution_date = context.get('execution_date', pendulum.now())
        
        subject = f"✅ Sucesso na DAG {dag_id} - Tarefa {task_id}"
        message = f"""Tarefa executada com sucesso:
DAG: {dag_id}
Tarefa: {task_id}
Data: {execution_date}
Pipeline concluído com sucesso!"""
        
        success = alert_system.send_email_alert(subject, message)
        
        if success:
            logger.info(f"✅ Email de sucesso enviado para {dag_id}.{task_id}")
        else:
            logger.info(f"✅ Tarefa {task_id} concluída (email não enviado)")
            
    except Exception as e:
        logger.error(f"❌ Erro no sistema de alertas de sucesso: {e}")

def dag_failure_callback(context):
    """Callback para falhas globais da DAG."""
    try:
        alert_system = AlertSystem()
        dag_id = context['dag'].dag_id
        execution_date = context['execution_date']
        error_message = str(context.get('exception', 'Erro desconhecido'))
        
        logger.critical(f"❌ FALHA GLOBAL na DAG {dag_id}: {error_message}")
        
        alert_system.send_email_alert(
            f"🚨 FALHA GLOBAL - DAG {dag_id}",
            f"""Falha global no pipeline ETL:
DAG: {dag_id}
Data: {execution_date}
Erro: {error_message}
Status: Pipeline completamente parado
Ação: Intervenção imediata necessária""",
            to_emails=["bugdroidgamesbr@gmail.com", "nando.devs@gmail.com"]
        )
            
    except Exception as e:
        logger.error(f"Erro no callback de falha global: {e}")

def dag_success_callback(context):
    """Callback para sucesso global da DAG."""
    try:
        alert_system = AlertSystem()
        dag_id = context['dag'].dag_id
        execution_date = context['execution_date']
        
        logger.info(f"✅ DAG {dag_id} concluída com sucesso")
        
        duration = context['dag_run'].duration
        
        alert_system.send_email_alert(
            f"✅ SUCESSO - DAG {dag_id} Concluída",
            f"""Pipeline ETL executado com sucesso:
DAG: {dag_id}
Data: {execution_date}
Status: Todos os dados processados com sucesso
Tempo de execução: {duration} segundos""",
            to_emails=["bugdroidgamesbr@gmail.com", "nando.devs@gmail.com"]
        )
            
    except Exception as e:
        logger.error(f"Erro no callback de sucesso global: {e}")

# ---
# Definição da DAG

with DAG(
    dag_id="desafio_etl_maxinutri",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl", "desafio"],
    on_failure_callback=dag_failure_callback,
    on_success_callback=dag_success_callback,
    default_args={
        'on_failure_callback': on_failure_callback,
        #'on_success_callback': False # on_success_callback,
        'email_on_failure': False, # Desativar emails padrão do Airflow
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5),
        'execution_timeout': pendulum.duration(minutes=120),
    }
) as dag:
    
    def run_extract_task():
        """Executa a extração dos dados."""
        try:
            logger.info("Iniciando extração de dados da API...")
            extract_main()
            logger.info("✅ Extração concluída com sucesso.")
        except Exception as e:
            logger.error(f"❌ Erro na extração: {e}")
            raise

    def run_transform_task():
        """Executa a transformação dos dados."""
        try:
            logger.info("Iniciando transformação de dados...")
            transform_main()
            logger.info("✅ Transformação concluída com sucesso.")
        except Exception as e:
            logger.error(f"❌ Erro na transformação: {e}")
            raise

    def run_create_database():
        """Cria o banco de dados se ele não existir."""
        hook = PostgresHook(postgres_conn_id="postgres-default")
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            db_name = "desafio_db"

            cur.execute(SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (db_name,))
            exists = cur.fetchone()

            if not exists:
                logger.info(f"Criando o banco de dados '{db_name}'...")
                cur.execute(SQL("CREATE DATABASE {}").format(Identifier(db_name)))
                logger.info(f"✅ Banco de dados '{db_name}' criado com sucesso.")
            else:
                logger.info(f"📊 Banco de dados '{db_name}' já existe.")

        except OperationalError as e:
            logger.error(f"❌ Erro de conexão ao criar banco: {e}")
            raise
        except ProgrammingError as e:
            logger.error(f"❌ Erro de SQL ao criar banco: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao criar banco: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def run_load_task():
        """Cria a conexão e executa o carregamento dos dados no banco de dados!"""
        conn = None
        cur = None
        try:
            hook = PostgresHook(postgres_conn_id="postgres-default")
            conn = hook.get_conn()
            conn.autocommit = False
            
            cur = conn.cursor()
            cur.execute("SET statement_timeout = 300000;")
            
            logger.info("Iniciando carga de dados...")
            load_main(cur=cur)
            conn.commit()
            logger.info("✅ Dados carregados com sucesso.")
            
        except (OperationalError, InterfaceError) as e:
            logger.error(f"❌ Erro de conexão durante carga: {e}")
            if conn:
                conn.rollback()
            raise
        except DataError as e:
            logger.error(f"❌ Erro de dados durante carga: {e}")
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado durante carga: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def validate_etl_process():
        """Validação simplificada do ETL."""
        hook = PostgresHook(postgres_conn_id="postgres-default", schema="desafio_db")
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
            
            checks = [
                ("Tabelas existem", """
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name IN ('dim_cliente', 'dim_produto', 'dim_tempo', 'dim_avaliacao', 'fato_pedido')
                """),
                ("Total de clientes", "SELECT COUNT(*) FROM dim_cliente"),
                ("Total de produtos", "SELECT COUNT(*) FROM dim_produto"),
                ("Total de pedidos", "SELECT COUNT(*) FROM fato_pedido"),
                ("Pedidos válidos", "SELECT COUNT(*) FROM fato_pedido WHERE preco > 0 AND frete >= 0")
            ]
            
            results = {}
            
            for check_name, query in checks:
                try:
                    cur.execute(query)
                    result = cur.fetchone()[0]
                    results[check_name] = result
                    logger.info(f"{check_name}: {result}")
                except Exception as e:
                    logger.warning(f"Falha na verificação '{check_name}': {e}")
                    results[check_name] = f"Erro: {e}"
            
            if results.get("Tabelas existem", 0) < 5:
                raise Exception("Não todas as tabelas foram criadas corretamente")
            
            if results.get("Total de pedidos", 0) == 0:
                logger.warning("⚠️ Tabela de pedidos está vazia")
            
            if results.get("Total de clientes", 0) == 0:
                logger.warning("⚠️ Tabela de clientes está vazia")
            
            if results.get("Total de produtos", 0) == 0:
                logger.warning("⚠️ Tabela de produtos está vazia")
            
            logger.info("✅ Validação concluída com sucesso")
            logger.info(f"📊 Resultados: {results}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Falha na validação: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def send_final_report():
        """Envia relatório final do processamento."""
        try:
            alert_system = AlertSystem()
            alert_system.send_email_alert(
                "📊 Relatório Diário - ETL Concluído",
                """Processamento ETL diário concluído com sucesso!
✅ Extração: Dados extraídos da API
✅ Transformação: Dados limpos e processados
✅ Carga: Dados carregados no Data Warehouse
✅ Validação: Qualidade dos dados verificada
Status: Pipeline completo executado com sucesso""",
                to_emails=["bugdroidgamesbr@gmail.com", "nando.devs@gmail.com"]
            )
            logger.info("✅ Relatório final enviado")
            
        except Exception as e:
            logger.warning(f"⚠️ Falha ao enviar relatório: {e}")

    # Definindo as tarefas
    extract_task = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=run_extract_task,
        retries=2,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=30),
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform_task,
        retries=2,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=30),
    )

    create_db_task = PythonOperator(
        task_id="create_database_if_not_exists",
        python_callable=run_create_database,
        retries=2,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=5),
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=run_load_task,
        retries=3,
        retry_delay=pendulum.duration(seconds=60),
        execution_timeout=pendulum.duration(minutes=60),
    )

    validate_task = PythonOperator(
        task_id="validate_etl_process",
        python_callable=validate_etl_process,
        retries=1,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=10),
    )

    report_task = PythonOperator(
        task_id="send_final_report",
        python_callable=send_final_report,
        retries=1,
        retry_delay=pendulum.duration(seconds=15),
        execution_timeout=pendulum.duration(minutes=5),
    )

    # Definindo as dependências
    extract_task >> transform_task >> create_db_task >> load_task >> validate_task >> report_task