
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
CREATE INDEX IF NOT EXISTS idx_dim_cliente_cidade ON dim_cliente(cidade);

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
CREATE INDEX IF NOT EXISTS idx_dim_produto_peso ON dim_produto(peso_g);

-- ==========================
-- DIMENSÃO TEMPO
-- ==========================
CREATE TABLE IF NOT EXISTS dim_tempo (
    tempo_sk BIGSERIAL PRIMARY KEY,
    data DATE UNIQUE NOT NULL,
    ano SMALLINT,              -- SMALLINT é suficiente para anos
    mes SMALLINT,              -- SMALLINT é suficiente para meses (1-12)
    dia SMALLINT,              -- SMALLINT é suficiente para dias (1-31)
    dia_da_semana VARCHAR(20),
    trimestre SMALLINT,
    semana_ano SMALLINT
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dim_tempo_data ON dim_tempo(data);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dim_tempo(ano, mes);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes_dia ON dim_tempo(ano, mes, dia);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_dia_semana ON dim_tempo(dia_da_semana);

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
CREATE INDEX IF NOT EXISTS idx_dim_avaliacao_title ON dim_avaliacao(review_comment_title);
CREATE INDEX IF NOT EXISTS idx_dim_avaliacao_message ON dim_avaliacao(review_comment_message);

-- ==========================
-- FATO PEDIDO
-- ==========================
CREATE TABLE IF NOT EXISTS fato_pedido (
    pedido_id VARCHAR(255) PRIMARY KEY,
    cliente_sk BIGINT REFERENCES dim_cliente(cliente_sk) ON DELETE CASCADE,
    produto_sk BIGINT REFERENCES dim_produto(produto_sk) ON DELETE CASCADE,
    tempo_sk BIGINT REFERENCES dim_tempo(tempo_sk) ON DELETE CASCADE,
    avaliacao_sk BIGINT REFERENCES dim_avaliacao(avaliacao_sk) ON DELETE CASCADE,
    status_pedido VARCHAR(50),
    preco DECIMAL(10, 2),
    frete DECIMAL(10, 2),
    data_aprovacao TIMESTAMP,
    data_entrega_transportadora TIMESTAMP,
    data_entrega_cliente TIMESTAMP,
    data_entrega_estimada TIMESTAMP,
    quantidade_itens SMALLINT DEFAULT 1,
    valor_total DECIMAL(12, 2) GENERATED ALWAYS AS (preco + frete) STORED
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_fato_cliente_sk ON fato_pedido(cliente_sk);
CREATE INDEX IF NOT EXISTS idx_fato_produto_sk ON fato_pedido(produto_sk);
CREATE INDEX IF NOT EXISTS idx_fato_tempo_sk ON fato_pedido(tempo_sk);
CREATE INDEX IF NOT EXISTS idx_fato_avaliacao_sk ON fato_pedido(avaliacao_sk);
CREATE INDEX IF NOT EXISTS idx_fato_status_pedido ON fato_pedido(status_pedido);
CREATE INDEX IF NOT EXISTS idx_fato_datas ON fato_pedido(data_aprovacao, data_entrega_cliente);
CREATE INDEX IF NOT EXISTS idx_fato_preco ON fato_pedido(preco);
CREATE INDEX IF NOT EXISTS idx_fato_frete ON fato_pedido(frete);
CREATE INDEX IF NOT EXISTS idx_fato_valor_total ON fato_pedido(valor_total);

-- ==========================
-- CONSTRAINTS ADICIONAIS
-- ==========================
ALTER TABLE fato_pedido ADD CONSTRAINT fk_fato_cliente 
    FOREIGN KEY (cliente_sk) REFERENCES dim_cliente(cliente_sk);

ALTER TABLE fato_pedido ADD CONSTRAINT fk_fato_produto 
    FOREIGN KEY (produto_sk) REFERENCES dim_produto(produto_sk);

ALTER TABLE fato_pedido ADD CONSTRAINT fk_fato_tempo 
    FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk);

ALTER TABLE fato_pedido ADD CONSTRAINT fk_fato_avaliacao 
    FOREIGN KEY (avaliacao_sk) REFERENCES dim_avaliacao(avaliacao_sk);

-- ==========================
-- VIEWS PARA CONSULTA
-- ==========================
CREATE OR REPLACE VIEW vw_pedidos_completos AS
SELECT 
    fp.pedido_id,
    fp.status_pedido,
    fp.preco,
    fp.frete,
    fp.valor_total,
    fp.data_aprovacao,
    fp.data_entrega_cliente,
    dc.cliente_id,
    dc.cidade as cidade_cliente,
    dc.estado as estado_cliente,
    dp.produto_id,
    dp.categoria as categoria_produto,
    dt.data,
    dt.ano,
    dt.mes,
    da.review_score,
    da.review_comment_title
FROM fato_pedido fp
LEFT JOIN dim_cliente dc ON fp.cliente_sk = dc.cliente_sk
LEFT JOIN dim_produto dp ON fp.produto_sk = dp.produto_sk
LEFT JOIN dim_tempo dt ON fp.tempo_sk = dt.tempo_sk
LEFT JOIN dim_avaliacao da ON fp.avaliacao_sk = da.avaliacao_sk;

-- ==========================
-- COMENTÁRIOS DAS TABELAS (DOCUMENTAÇÃO)
-- ==========================
COMMENT ON TABLE dim_cliente IS 'Dimensão de clientes com informações demográficas';
COMMENT ON TABLE dim_produto IS 'Dimensão de produtos com características físicas';
COMMENT ON TABLE dim_tempo IS 'Dimensão temporal para análise por data';
COMMENT ON TABLE dim_avaliacao IS 'Dimensão de avaliações dos produtos';
COMMENT ON TABLE fato_pedido IS 'Tabela de fatos com métricas de pedidos e vendas';

COMMENT ON COLUMN dim_cliente.cep_prefix IS 'Prefixos de CEP para análise geográfica';
COMMENT ON COLUMN fato_pedido.valor_total IS 'Valor total calculado (preço + frete)';

-- ==========================
-- TABELA DE LOG/MONITORAMENTO
-- ==========================
CREATE TABLE IF NOT EXISTS etl_log (
    log_id BIGSERIAL PRIMARY KEY,
    tabela VARCHAR(100),
    operacao VARCHAR(50),
    registros_afetados INTEGER,
    data_execucao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    mensagem TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_log_data ON etl_log(data_execucao);
CREATE INDEX IF NOT EXISTS idx_etl_log_tabela ON etl_log(tabela);