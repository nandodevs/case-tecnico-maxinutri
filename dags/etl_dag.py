# dags/etl_dag.py
from __future__ import annotations
import datetime
import pendulum
import sys
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.email import send_email
from psycopg2.sql import SQL, Identifier
from pathlib import Path
import logging, traceback
from airflow.exceptions import AirflowException
from psycopg2 import OperationalError, ProgrammingError, DataError, InterfaceError

logger = logging.getLogger(__name__)

# Adiciona o diretório `etl` ao PYTHONPATH para que os scripts possam ser importados
sys.path.append(str(Path(__file__).parent.parent / "etl"))

# Importa as funções principais dos seus scripts
from extract import main as extract_main
from transform import main as transform_main
from load import main as load_main

# Callback para falhas - ENVIA EMAIL REAL
def send_alert_on_failure(context: Context):
    """
    Função para enviar alerta em caso de falha na tarefa
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context.get('execution_date', 'N/A')
        
        # Formata a data se estiver disponível
        if execution_date != 'N/A' and hasattr(execution_date, 'strftime'):
            execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            execution_date_str = str(execution_date)
        
        # Captura a exceção real e traceback completo
        exception = context.get('exception')
        if exception:
            error_message = str(exception)
            traceback_msg = ''.join(traceback.format_exception(
                type(exception), exception, exception.__traceback__
            ))
        else:
            error_message = "Erro desconhecido (sem exceção capturada)"
            traceback_msg = "Nenhum traceback disponível"
        
        # Log do erro
        logger.error(f"❌ Falha na tarefa {task_id} do DAG {dag_id}")
        logger.error(f"📅 Data de execução: {execution_date_str}")
        logger.error(f"💡 Mensagem de erro: {error_message}")
        
        # Email com detalhes técnicos completos
        subject = f"🚨 ALERTA: Falha no Airflow - DAG: {dag_id}, Tarefa: {task_id}"
        body = f"""
        <h3>🚨 Falha detectada no processo ETL</h3>
        
        <strong>📋 Detalhes da Tarefa:</strong><br>
        • <strong>DAG:</strong> {dag_id}<br>
        • <strong>Tarefa:</strong> {task_id}<br>
        • <strong>Data de execução:</strong> {execution_date_str}<br><br>
        
        <strong>❌ Mensagem de Erro:</strong><br>
        <pre>{error_message}</pre><br>
        
        <strong>🔍 Detalhe Técnico Completo:</strong><br>
        <pre>{traceback_msg}</pre><br>
        
        <strong>📊 Status:</strong> FALHA<br>
        <strong>🕒 Hora da falha:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br><br>
        
        <em>Por favor, verifique os logs do Airflow para mais detalhes.</em>
        """
        
        logger.info(f"📧 Tentando enviar email de alerta: {subject}")
        
        # ENVIA EMAIL REAL
        try:
            send_email(
                to=['bugdroidgamesbr@gmail.com', 'nando.devs@gmail.com'],
                subject=subject,
                html_content=body
            )
            logger.info("✅ Email de alerta enviado com sucesso!")
        except Exception as email_error:
            logger.error(f"❌ Falha ao enviar email: {email_error}")
            logger.error(traceback.format_exc())
        
    except Exception as e:
        logger.error(f"❌ Erro no sistema de alertas: {str(e)}")
        logger.error(traceback.format_exc())

# Callback para sucesso do DAG completo
def dag_success_callback(context: Context):
    """Callback para quando o DAG inteiro é executado com sucesso"""
    try:
        dag_id = context['dag'].dag_id
        execution_date = context.get('execution_date', 'N/A')
        
        if execution_date != 'N/A' and hasattr(execution_date, 'strftime'):
            execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            execution_date_str = str(execution_date)
        
        subject = f"✅ SUCESSO: DAG {dag_id} executado com sucesso"
        body = f"""
        <h3>✅ Processamento ETL Concluído com Sucesso</h3>
        
        <strong>📋 Detalhes:</strong><br>
        • <strong>DAG:</strong> {dag_id}<br>
        • <strong>Data de execução:</strong> {execution_date_str}<br>
        • <strong>Status:</strong> SUCESSO COMPLETO<br>
        • <strong>Hora de conclusão:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br><br>
        
        <strong>📊 Fluxo executado:</strong><br>
        ✅ Extração de dados da API<br>
        ✅ Transformação e limpeza dos dados<br>
        ✅ Criação do banco de dados<br>
        ✅ Carga no Data Warehouse<br>
        ✅ Validação da qualidade<br>
        ✅ Relatório final<br><br>
        
        <em>Todas as tarefas foram executadas com sucesso!</em>
        """
        
        logger.info(f"📧 Tentando enviar email de sucesso: {subject}")
        
        try:
            send_email(
                to=['bugdroidgamesbr@gmail.com', 'nando.devs@gmail.com'],
                subject=subject,
                html_content=body
            )
            logger.info("✅ Email de sucesso enviado!")
        except Exception as email_error:
            logger.error(f"❌ Falha ao enviar email de sucesso: {email_error}")
                
    except Exception as e:
        logger.warning(f"⚠️ Erro no callback de sucesso do DAG: {e}")

# Configuração dos callbacks
on_failure_callback = send_alert_on_failure

with DAG(
    dag_id="case_tecnico_maxinutri",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl", "desafio"],
    on_success_callback=dag_success_callback,  # Callback para sucesso do DAG completo
    default_args={
        'owner': "Sisnando Junior",
        'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
        'on_failure_callback': on_failure_callback,
        'email_on_failure': False,  # Desativa emails padrão do Airflow
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
            logger.error(traceback.format_exc())
            raise

    def run_transform_task():
        """Executa a transformação dos dados."""
        try:
            logger.info("Iniciando transformação de dados...")
            transform_main()
            logger.info("✅ Transformação concluída com sucesso.")
        except Exception as e:
            logger.error(f"❌ Erro na transformação: {e}")
            logger.error(traceback.format_exc())
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
            logger.error(traceback.format_exc())
            raise
        except ProgrammingError as e:
            logger.error(f"❌ Erro de SQL ao criar banco: {e}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao criar banco: {e}")
            logger.error(traceback.format_exc())
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
            hook = PostgresHook(postgres_conn_id="postgres-default")
            conn = hook.get_conn()
            conn.autocommit = False
            
            # Configura timeout para evitar operações muito longas
            cur = conn.cursor()
            cur.execute("SET statement_timeout = 300000;")  # 5 minutos
            
            logger.info("Iniciando carga de dados...")
            load_main(cur=cur)
            conn.commit()
            logger.info("✅ Dados carregados com sucesso.")
            
        except (OperationalError, InterfaceError) as e:
            logger.error(f"❌ Erro de conexão durante carga: {e}")
            logger.error(traceback.format_exc())
            if conn:
                conn.rollback()
            raise
        except DataError as e:
            logger.error(f"❌ Erro de dados durante carga: {e}")
            logger.error(traceback.format_exc())
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado durante carga: {e}")
            logger.error(traceback.format_exc())
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
            
            # Lista de verificações a serem realizadas
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
            
            # Verificar se todas as tabelas essenciais existem
            if results.get("Tabelas existem", 0) < 5:
                raise Exception("Não todas as tabelas foram criadas corretamente")
            
            # Verificar se há dados nas tabelas principais
            if results.get("Total de pedidos", 0) == 0:
                logger.warning("⚠️  Tabela de pedidos está vazia")
            
            if results.get("Total de clientes", 0) == 0:
                logger.warning("⚠️  Tabela de clientes está vazia")
            
            if results.get("Total de produtos", 0) == 0:
                logger.warning("⚠️  Tabela de produtos está vazia")
            
            logger.info("✅ Validação concluída com sucesso")
            logger.info(f"📊 Resultados: {results}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Falha na validação: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def send_final_report():
        """Envia relatório final do processamento."""
        try:
            logger.info("📊 Preparando relatório final do ETL...")
            
            # Simula envio de relatório
            logger.info("✅ Processamento ETL concluído com sucesso!")
            logger.info("📋 Relatório:")
            logger.info("  ✅ Extração: Dados extraídos da API")
            logger.info("  ✅ Transformação: Dados limpos e processados")
            logger.info("  ✅ Carga: Dados carregados no Data Warehouse")
            logger.info("  ✅ Validação: Qualidade dos dados verificada")
            logger.info("  📊 Status: Pipeline completo executado com sucesso")
                
        except Exception as e:
            logger.warning(f"⚠️ Falha ao enviar relatório: {e}")
            logger.error(traceback.format_exc())

    # Tarefa de teste para verificar alertas
    def test_failure_task():
        """Tarefa de teste que falha propositalmente"""
        raise ConnectionError("❌ ERRO DE CONEXÃO: Falha simulada para testar alertas de email - Timeout na conexão com PostgreSQL")

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

    # test_task = PythonOperator(
    #     task_id="test_alert_system",
    #     python_callable=test_failure_task,
    #     retries=0,
    #     execution_timeout=pendulum.duration(minutes=2),
    # )

    # Definindo as dependências principais
    extract_task >> transform_task >> create_db_task >> load_task >> validate_task >> report_task

    # ⚠️ PARA TESTAR ALERTAS: Descomente a linha abaixo
    # test_task  # Remove o comentário para testar os alertas de email