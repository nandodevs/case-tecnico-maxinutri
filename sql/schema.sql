-- ==========================
-- DIMENSÃO CLIENTE
-- ==========================
CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_sk BIGSERIAL PRIMARY KEY,
    cliente_id VARCHAR(255) UNIQUE NOT NULL,
    cidade VARCHAR(255),
    estado VARCHAR(255),
    cep_prefix VARCHAR(10)  -- CEP como VARCHAR para preservar zeros à esquerda
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dim_cliente_id ON dim_cliente(cliente_id);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_estado ON dim_cliente(estado);

-- ==========================
-- DIMENSÃO PRODUTO
-- ==========================
CREATE TABLE IF NOT EXISTS dim_produto (
    produto_sk BIGSERIAL PRIMARY KEY,
    produto_id VARCHAR(255) UNIQUE NOT NULL,
    categoria VARCHAR(255),
    peso_g DECIMAL(10,3),         -- DECIMAL para suportar valores maiores e decimais
    comprimento_cm DECIMAL(10,2), -- DECIMAL para suportar decimais
    altura_cm DECIMAL(10,2),      -- DECIMAL para suportar decimais
    largura_cm DECIMAL(10,2),     -- DECIMAL para suportar decimais
    fotos_qty SMALLINT             -- SMALLINT é suficiente para quantidade de fotos (0-32767)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dim_produto_id ON dim_produto(produto_id);
CREATE INDEX IF NOT EXISTS idx_dim_produto_categoria ON dim_produto(categoria);

-- ==========================
-- DIMENSÃO TEMPO
-- ==========================
CREATE TABLE IF NOT EXISTS dim_tempo (
    tempo_sk BIGSERIAL PRIMARY KEY,
    data DATE UNIQUE NOT NULL,
    ano SMALLINT,              -- SMALLINT é suficiente para anos
    mes SMALLINT,              -- SMALLINT é suficiente para meses (1-12)
    dia SMALLINT,              -- SMALLINT é suficiente para dias (1-31)
    dia_da_semana VARCHAR(20)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dim_tempo_data ON dim_tempo(data);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dim_tempo(ano, mes);

-- ==========================
-- DIMENSÃO AVALIAÇÃO
-- ==========================
CREATE TABLE IF NOT EXISTS dim_avaliacao (
    avaliacao_sk BIGSERIAL PRIMARY KEY,
    review_score SMALLINT,     -- SMALLINT é suficiente para scores (geralmente 1-5)
    review_comment_title TEXT,
    review_comment_message TEXT,
    UNIQUE (review_score, review_comment_title, review_comment_message)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dim_avaliacao_score ON dim_avaliacao(review_score);

-- ==========================
-- FATO PEDIDO
-- ==========================
CREATE TABLE IF NOT EXISTS fato_pedido (
    pedido_id VARCHAR(255) PRIMARY KEY,
    cliente_sk BIGINT REFERENCES dim_cliente(cliente_sk),
    produto_sk BIGINT REFERENCES dim_produto(produto_sk),
    tempo_sk BIGINT REFERENCES dim_tempo(tempo_sk),
    avaliacao_sk BIGINT REFERENCES dim_avaliacao(avaliacao_sk),
    status_pedido VARCHAR(50),
    preco DECIMAL(10, 2),
    frete DECIMAL(10, 2),
    data_aprovacao TIMESTAMP,
    data_entrega_transportadora TIMESTAMP,
    data_entrega_cliente TIMESTAMP,
    data_entrega_estimada TIMESTAMP
);

-- Índices para performance em joins e filtros
CREATE INDEX IF NOT EXISTS idx_fato_cliente_sk ON fato_pedido(cliente_sk);
CREATE INDEX IF NOT EXISTS idx_fato_produto_sk ON fato_pedido(produto_sk);
CREATE INDEX IF NOT EXISTS idx_fato_tempo_sk ON fato_pedido(tempo_sk);
CREATE INDEX IF NOT EXISTS idx_fato_avaliacao_sk ON fato_pedido(avaliacao_sk);
CREATE INDEX IF NOT EXISTS idx_fato_status_pedido ON fato_pedido(status_pedido);
CREATE INDEX IF NOT EXISTS idx_fato_datas ON fato_pedido(data_aprovacao, data_entrega_cliente);
