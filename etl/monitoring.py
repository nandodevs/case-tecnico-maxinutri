"""
Módulo para monitoramento e alertas por email - Versão Corrigida
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import pendulum

logger = logging.getLogger(__name__)

class AlertSystem:
    def __init__(self):
        # Configurações padrão
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.mail_from = os.getenv('SMTP_MAIL_FROM', self.smtp_user)
        
        # Lista de destinatários padrão
        self.default_recipients = os.getenv('ALERT_RECIPIENTS', '').split(',')
        # Remover espaços e entradas vazias
        self.default_recipients = [email.strip() for email in self.default_recipients if email.strip()]
        
        # Se não houver destinatários configurados, usar o email de autenticação
        if not self.default_recipients:
            self.default_recipients = [self.smtp_user]
        
        self.email_enabled = all([self.smtp_host, self.smtp_user, self.smtp_password])
        
        if not self.email_enabled:
            logger.warning("Configuração de email incompleta. Alertas por email desativados.")
        else:
            logger.info("Sistema de alertas por email configurado")
    
    def send_email_alert(self, subject, message, to_emails=None, html_content=None):
        """
        Envia alerta por email com opção de conteúdo HTML - Versão Corrigida
        """
        if not self.email_enabled:
            logger.warning("Sistema de email desativado. Não é possível enviar alertas.")
            return False
            
        if to_emails is None:
            to_emails = self.default_recipients
            
        try:
            # Criar mensagem
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[AIRFLOW ALERT] {subject}"
            msg['From'] = self.mail_from
            msg['To'] = ', '.join(to_emails)
            
            # Criar versões texto e HTML
            text_part = MIMEText(message, 'plain', 'utf-8')
            msg.attach(text_part)
            
            if html_content:
                html_part = MIMEText(html_content, 'html', 'utf-8')
                msg.attach(html_part)
            
            # Enviar email - usando conexão mais simples
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.mail_from, to_emails, msg.as_string())
            server.quit()
                
            logger.info(f"✅ Alerta enviado para {to_emails}: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Falha ao enviar alerta por email: {e}")
            return False
    
    def create_html_alert(self, dag_id, task_id, error_message, execution_date, is_critical=False):
        """
        Cria conteúdo HTML para o alerta
        """
        critical_style = "background-color: #ff4444; color: white;" if is_critical else ""
        
        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="{critical_style} padding: 20px; border-radius: 5px; margin-bottom: 20px;">
                <h1 style="margin: 0;">{'🚨 FALHA CRÍTICA' if is_critical else '⚠️ Falha no Pipeline'}</h1>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; border-left: 4px solid {'#dc3545' if is_critical else '#ffc107'};">
                <h2 style="margin-top: 0;">{dag_id} - {task_id}</h2>
                
                <div style="background-color: white; padding: 15px; border-radius: 3px; margin: 15px 0;">
                    <h3 style="margin-top: 0;">📋 Detalhes da Execução</h3>
                    <p><strong>Data/Hora:</strong> {execution_date}</p>
                    <p><strong>Severidade:</strong> <span style="color: {'#dc3545' if is_critical else '#ffc107'}">{'CRÍTICA' if is_critical else 'NORMAL'}</span></p>
                </div>
                
                <div style="background-color: #fff3cd; padding: 15px; border-radius: 3px; margin: 15px 0;">
                    <h3 style="margin-top: 0;">🔍 Detalhes do Erro</h3>
                    <pre style="background-color: #f8f9fa; padding: 10px; border-radius: 3px; overflow: auto; white-space: pre-wrap;">{error_message}</pre>
                </div>
            </div>
            
            <div style="margin-top: 20px; padding: 15px; background-color: #e7f3ff; border-radius: 5px;">
                <h3 style="margin-top: 0;">🚀 Ações Recomendadas</h3>
                <ol>
                    <li>Verificar logs completos no Airflow</li>
                    <li>Validar conectividade com serviços externos</li>
                    <li>Checar métricas do sistema</li>
                    <li>Executar teste de integridade</li>
                </ol>
            </div>
        </div>
        """

# Instância global do sistema de alertas
alert_system = AlertSystem()

def on_failure_callback(context):
    """
    Callback para falhas de tarefas do Airflow - Versão Corrigida
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        exception = context.get('exception', 'Erro desconhecido')
        
        subject = f"Falha na DAG {dag_id} - Tarefa {task_id}"
        error_message = str(exception)
        
        # Determinar severidade
        is_critical = any(keyword in error_message.lower() for keyword in 
                         ['connection', 'database', 'timeout', 'critical', 'urgent'])
        
        # Mensagem simples
        simple_message = f"""Falha no pipeline ETL:

DAG: {dag_id}
Tarefa: {task_id}
Data: {execution_date}
Severidade: {'CRÍTICA' if is_critical else 'Normal'}

Erro: {error_message}

Acesse o Airflow para mais detalhes."""
        
        # Conteúdo HTML
        html_content = alert_system.create_html_alert(dag_id, task_id, error_message, execution_date, is_critical)
        
        # Enviar alerta
        success = alert_system.send_email_alert(
            subject, 
            simple_message, 
            html_content=html_content
        )
        
        if success:
            logger.info(f"✅ Alerta de falha enviado para {dag_id}.{task_id}")
        else:
            logger.warning(f"⚠️ Falha ao enviar alerta de email para {dag_id}.{task_id}")
        
        logger.error(f"❌ Falha na tarefa {task_id} da DAG {dag_id}: {error_message}")
        
    except Exception as e:
        logger.error(f"❌ Erro no sistema de alertas: {e}")

def on_success_callback(context):
    """
    Callback para sucesso de tarefas do Airflow - Versão Corrigida
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        
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
        
        logger.info(f"✅ Tarefa {task_id} da DAG {dag_id} concluída com sucesso")
        
    except Exception as e:
        logger.error(f"❌ Erro no sistema de alertas de sucesso: {e}")