-- Criação do Schema para o Modelo Estrela (CORREÇÃO FINAL)

-- Tabela de Dimensão de Clientes
-- Armazena informações descritivas sobre os clientes.
-- A chave 'cliente_sk' é uma chave substituta (surrogate key) para a tabela de fatos.
-- A chave 'cliente_id' é a chave de negócio (business key) e garante a unicidade dos clientes.
CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_sk BIGSERIAL PRIMARY KEY,
    cliente_id VARCHAR(255) UNIQUE NOT NULL,
    cidade VARCHAR(255),
    estado VARCHAR(255),
    cep_prefix VARCHAR(10)  -- CEP como VARCHAR para preservar zeros à esquerda
);

-- Tabela de Dimensão de Produtos
-- Contém atributos descritivos de cada produto.
CREATE TABLE IF NOT EXISTS dim_produto (
    produto_sk BIGSERIAL PRIMARY KEY,
    produto_id VARCHAR(255) UNIQUE NOT NULL,
    categoria VARCHAR(255),
    peso_g DECIMAL(10,3),      -- DECIMAL para suportar valores maiores e decimais
    comprimento_cm DECIMAL(10,2), -- DECIMAL para suportar decimais
    altura_cm DECIMAL(10,2),   -- DECIMAL para suportar decimais
    largura_cm DECIMAL(10,2),  -- DECIMAL para suportar decimais
    fotos_qty SMALLINT         -- SMALLINT é suficiente para quantidade de fotos (0-32767)
);

-- Tabela de Dimensão de Tempo
-- A data é a chave de negócio e a chave substituta 'tempo_sk' é usada na tabela de fatos.
-- É útil para analisar dados ao longo do tempo.
CREATE TABLE IF NOT EXISTS dim_tempo (
    tempo_sk BIGSERIAL PRIMARY KEY,
    data DATE UNIQUE NOT NULL,
    ano SMALLINT,              -- SMALLINT é suficiente para anos
    mes SMALLINT,              -- SMALLINT é suficiente para meses (1-12)
    dia SMALLINT,              -- SMALLINT é suficiente para dias (1-31)
    dia_da_semana VARCHAR(20)
);

-- Tabela de Dimensão de Avaliação
-- Normaliza os dados de avaliação para evitar redundância na tabela de fatos.
CREATE TABLE IF NOT EXISTS dim_avaliacao (
    avaliacao_sk BIGSERIAL PRIMARY KEY,
    review_score SMALLINT,     -- SMALLINT é suficiente para scores (geralmente 1-5)
    review_comment_title TEXT,
    review_comment_message TEXT,
    -- Uma restrição de unicidade para evitar a duplicação de avaliações idênticas
    UNIQUE (review_score, review_comment_title, review_comment_message)
);

-- Tabela de Fatos de Pedidos
-- É o núcleo do modelo, armazenando as métricas e chaves estrangeiras para as dimensões.
CREATE TABLE IF NOT EXISTS fato_pedido (
    pedido_id VARCHAR(255) PRIMARY KEY,  -- CORRIGIDO: VARCHAR em vez de BIGINT
    -- Chaves estrangeiras para conectar às tabelas de dimensão
    cliente_sk BIGINT REFERENCES dim_cliente(cliente_sk),
    produto_sk BIGINT REFERENCES dim_produto(produto_sk),
    tempo_sk BIGINT REFERENCES dim_tempo(tempo_sk),
    avaliacao_sk BIGINT REFERENCES dim_avaliacao(avaliacao_sk),
    -- Métricas e atributos de fato
    status_pedido VARCHAR(50),
    preco DECIMAL(10, 2),
    frete DECIMAL(10, 2),
    data_aprovacao TIMESTAMP,
    data_entrega_transportadora TIMESTAMP,
    data_entrega_cliente TIMESTAMP,
    data_entrega_estimada TIMESTAMP
);