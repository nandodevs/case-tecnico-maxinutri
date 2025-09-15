"""
Módulo para monitoramento e alertas por email
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import pendulum
from typing import Optional, List

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
        self.default_recipients = [email.strip() for email in os.getenv('ALERT_RECIPIENTS', '').split(',') if email.strip()]
        
        if not self.default_recipients and self.smtp_user:
            self.default_recipients = [self.smtp_user]
        
        self.email_enabled = all([self.smtp_host, self.smtp_user, self.smtp_password])
        
        if not self.email_enabled:
            logger.warning("Configuração de email incompleta. Alertas por email desativados.")
        else:
            logger.info("Sistema de alertas por email configurado")

    def send_email_alert(self, subject: str, message: str, to_emails: Optional[List[str]] = None, html_content: Optional[str] = None) -> bool:
        """
        Envia alerta por email com opção de conteúdo HTML
        """
        if not self.email_enabled:
            logger.warning("Sistema de email desativado. Não é possível enviar alertas.")
            return False
            
        if to_emails is None:
            to_emails = self.default_recipients
        
        if not to_emails:
            logger.warning("Nenhum destinatário especificado. Não é possível enviar alerta.")
            return False
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[AIRFLOW ALERT] {subject}"
            msg['From'] = self.mail_from
            msg['To'] = ', '.join(to_emails)
            
            text_part = MIMEText(message, 'plain', 'utf-8')
            msg.attach(text_part)
            
            if html_content:
                html_part = MIMEText(html_content, 'html', 'utf-8')
                msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.mail_from, to_emails, msg.as_string())
                
            logger.info(f"✅ Alerta enviado para {to_emails}: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Falha ao enviar alerta por email: {e}")
            return False
    
    def create_html_alert(self, dag_id: str, task_id: str, error_message: str, execution_date: pendulum.DateTime, is_critical: bool = False) -> str:
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