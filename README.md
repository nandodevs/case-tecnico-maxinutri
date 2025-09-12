# Case Técnico - ETL Maxinutri

Resumo
- Projeto de ETL com Apache Airflow para extração, transformação e carga de dados.
- Diferencial: uso de Docker para orquestrar o banco PostgreSQL.
- Este repositório contém DAGs, scripts ETL (pasta `etl/`), configurações do Airflow e documentação.

Conteúdo
- dags/ : DAGs do Airflow
- etl/ : scripts Python de extract/transform/load
- .astro/config.yaml : configuração do Astro CLI (porta do webserver)
- airflow_settings.yaml : conexões/variáveis para importar no Airflow
- docs/ : documentação detalhada

Configuração da Conexão na UI do Airflow
- Postgres:
    - conn_id: meu_postgres
    - conn_type: postgres
    - description: "Conexão para o banco 'airflow_db' no PostgreSQL"
    - host: localhost
    - port: 5544
    - schema: desafio_db
    - login: airflow
    - password: airflow123

Quickstart (usando docker-compose)
1. Crie as pastas locais:
   - `mkdir -p dags logs plugins scripts sql`
2. Inicie containers:
   - `docker compose up -d`
3. Inicialize o banco e crie o usuário Airflow:
   - (o serviço `airflow-init` do docker-compose já executa `airflow db upgrade` e cria um usuário padrão)
4. Acesse o Airflow Web UI:
   - http://localhost:8080 (ou porta configurada em `.astro/config.yaml` / `webserver_port`)
5. Importe/Crie a conexão:
   - Use `Admin -> Connections` e crie `meu_postgres` conforme `airflow_settings.yaml` ou importe o arquivo.

Executar a DAG de teste
- Ative a DAG `teste_cria_tabela_postgres` no Airflow UI e dispare manualmente.
- A DAG executará um comando SQL para criar a tabela `teste_tabela`.

Verificação (banco)
- Do host:
  - `psql -h localhost -p 5544 -U airflow -d desafio_db`
  - `\dt` e `SELECT * FROM teste_tabela LIMIT 10;`

Gerar ERD (diagrama do esquema)
- Use a query em `docs/db_schema.md` para extrair FKs do banco automaticamente.
- Ferramentas sugeridas: dbdiagram.io, Mermaid, Graphviz, DBeaver, SchemaSpy.

Documentação adicional
- Veja `docs/architecture.md`, `docs/airflow_config.md`, `docs/db_schema.md`, `docs/testing.md`.

Checklist para o avaliador
- [ ] Containers sobem sem erros (`docker compose ps`)
- [ ] Conexão `meu_postgres` existe no Airflow e está funcional
- [ ] DAG `teste_cria_tabela_postgres` executa com sucesso e cria `teste_tabela`
- [ ] ERD do banco disponível ou gerado com instruções

Observações
- Não use essas credenciais em produção.
- Se você usa Astro CLI, veja `.astro/config.yaml` e `airflow_settings.yaml` para importar configurações.