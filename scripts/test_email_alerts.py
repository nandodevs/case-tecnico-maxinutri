<<<<<<< HEAD
#!/usr/bin/env python3
"""
Script para testar o sistema de alertas por email - Versão Simplificada
"""
import sys
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Adicionar o diretório raiz ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_smtp_connection():
    """Testa a conexão SMTP diretamente"""
    try:
        import smtplib
        
        smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        smtp_user = os.getenv('SMTP_USER', '')
        smtp_password = os.getenv('SMTP_PASSWORD', '')
        critical_alerts = os.getenv('CRITICAL_ALERTS', '').split(',')
        critical_alerts = [email.strip() for email in critical_alerts if email.strip()]
        
        if not all([smtp_host, smtp_user, smtp_password]):
            logger.error("❌ Variáveis de ambiente SMTP não configuradas")
            return False
        
        logger.info(f"Testando conexão com {smtp_host}:{smtp_port}...")
        
        # Conexão direta sem context manager
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_password)
        
        logger.info("✅ Login SMTP bem-sucedido!")
        
        # Testar envio de email simples
        from email.mime.text import MIMEText
        
        msg = MIMEText("Este é um email de teste do sistema Airflow! Sistema de envio de erros no Airflow!", 'plain', 'utf-8')
        msg['Subject'] = 'Teste de Email - Airflow'
        msg['From'] = smtp_user
        msg['To'] = smtp_user
        
        server.sendmail(smtp_user, [smtp_user], msg.as_string())
        server.quit()
        
        logger.info("✅ Email de teste enviado com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no teste SMTP: {e}")
        return False

def test_env_variables():
    """Verifica se as variáveis de ambiente estão configuradas"""
    required_vars = ['SMTP_HOST', 'SMTP_USER', 'SMTP_PASSWORD']
    
    logger.info("Verificando variáveis de ambiente...")
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"✅ {var}: {value[:5]}...")  # Mostrar apenas primeiros caracteres por segurança
        else:
            logger.error(f"❌ {var}: Não configurada")
            return False
    
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("TESTE DO SISTEMA DE ALERTAS POR EMAIL")
    print("=" * 50)
    
    # Verificar variáveis de ambiente primeiro
    if not test_env_variables():
        print("\n❌ Configure as variáveis de ambiente primeiro:")
        print("export SMTP_HOST=smtp.gmail.com")
        print("export SMTP_PORT=587")
        print("export SMTP_USER=seu-email@gmail.com")
        print("export SMTP_PASSWORD=sua-senha-app")
        print("export SMTP_MAIL_FROM=seu-email@gmail.com")
        sys.exit(1)
    
    # Testar conexão SMTP
    print("\n" + "=" * 30)
    print("Testando conexão SMTP...")
    success = test_smtp_connection()
    
    if success:
        print("\n🎉 Sistema de email configurado com sucesso!")
    else:
        print("\n❌ Falha na configuração do email.")
        print("\n💡 Dicas de solução:")
        print("1. Verifique se a verificação em duas etapas está ativada no Gmail")
        print("2. Gere uma senha de app específica para o Airflow")
=======
#!/usr/bin/env python3
"""
Script para testar o sistema de alertas por email - Versão Simplificada
"""
import sys
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Adicionar o diretório raiz ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_smtp_connection():
    """Testa a conexão SMTP diretamente"""
    try:
        import smtplib
        
        smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        smtp_user = os.getenv('SMTP_USER', '')
        smtp_password = os.getenv('SMTP_PASSWORD', '')
        critical_alerts = os.getenv('CRITICAL_ALERTS', '').split(',')
        critical_alerts = [email.strip() for email in critical_alerts if email.strip()]
        
        if not all([smtp_host, smtp_user, smtp_password]):
            logger.error("❌ Variáveis de ambiente SMTP não configuradas")
            return False
        
        logger.info(f"Testando conexão com {smtp_host}:{smtp_port}...")
        
        # Conexão direta sem context manager
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_password)
        
        logger.info("✅ Login SMTP bem-sucedido!")
        
        # Testar envio de email simples
        from email.mime.text import MIMEText
        
        msg = MIMEText("Este é um email de teste do sistema Airflow! Sistema de envio de erros no Airflow!", 'plain', 'utf-8')
        msg['Subject'] = 'Teste de Email - Airflow'
        msg['From'] = smtp_user
        msg['To'] = smtp_user
        
        server.sendmail(smtp_user, [smtp_user], msg.as_string())
        server.quit()
        
        logger.info("✅ Email de teste enviado com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no teste SMTP: {e}")
        return False

def test_env_variables():
    """Verifica se as variáveis de ambiente estão configuradas"""
    required_vars = ['SMTP_HOST', 'SMTP_USER', 'SMTP_PASSWORD']
    
    logger.info("Verificando variáveis de ambiente...")
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"✅ {var}: {value[:5]}...")  # Mostrar apenas primeiros caracteres por segurança
        else:
            logger.error(f"❌ {var}: Não configurada")
            return False
    
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("TESTE DO SISTEMA DE ALERTAS POR EMAIL")
    print("=" * 50)
    
    # Verificar variáveis de ambiente primeiro
    if not test_env_variables():
        print("\n❌ Configure as variáveis de ambiente primeiro:")
        print("export SMTP_HOST=smtp.gmail.com")
        print("export SMTP_PORT=587")
        print("export SMTP_USER=seu-email@gmail.com")
        print("export SMTP_PASSWORD=sua-senha-app")
        print("export SMTP_MAIL_FROM=seu-email@gmail.com")
        sys.exit(1)
    
    # Testar conexão SMTP
    print("\n" + "=" * 30)
    print("Testando conexão SMTP...")
    success = test_smtp_connection()
    
    if success:
        print("\n🎉 Sistema de email configurado com sucesso!")
    else:
        print("\n❌ Falha na configuração do email.")
        print("\n💡 Dicas de solução:")
        print("1. Verifique se a verificação em duas etapas está ativada no Gmail")
        print("2. Gere uma senha de app específica para o Airflow")
>>>>>>> 8ef029d9c58e300164d0708fa1707f9c196cc777
        print("3. Confirme se o acesso a apps menos seguros está desativado")