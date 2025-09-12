# Case Técnico - Maxinutri

## Estrutura do Projeto

- **etl/**: Scripts de extração, transformação e carga
- **sql/**: Scripts SQL e DDL do Data Warehouse
- **airflow/**: DAGs e configurações do Airflow
- **data/**: Dados extraídos da API
- **docs/**: Documentação e diagramas

## Tecnologias
- Python
- PostgreSQL
- Apache Airflow

## Diagrama de Arquitetura

# Criar o banco no docker-compose.yml
docker run --name meu_postgres \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin@123 \
  -e POSTGRES_DB=desafiodb \
  -p 5433 \
  -d postgres:15
  
*(Adicionar diagrama em breve)*
