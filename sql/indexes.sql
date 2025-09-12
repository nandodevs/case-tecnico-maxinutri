-- ==========================
-- ÍNDICES DIM_CLIENTE
-- ==========================
CREATE INDEX IF NOT EXISTS idx_dim_cliente_id ON dim_cliente(cliente_id);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_estado ON dim_cliente(estado);

-- ==========================
-- ÍNDICES DIM_PRODUTO
-- ==========================
CREATE INDEX IF NOT EXISTS idx_dim_produto_id ON dim_produto(produto_id);
CREATE INDEX IF NOT EXISTS idx_dim_produto_categoria ON dim_produto(categoria);

-- ==========================
-- ÍNDICES DIM_TEMPO
-- ==========================
CREATE INDEX IF NOT EXISTS idx_dim_tempo_data ON dim_tempo(data);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dim_tempo(ano, mes);

-- ==========================
-- ÍNDICES DIM_AVALIACAO
-- ==========================
CREATE INDEX IF NOT EXISTS idx_dim_avaliacao_score ON dim_avaliacao(review_score);

-- ==========================
-- ÍNDICES FATO_PEDIDO
-- ==========================
CREATE INDEX IF NOT EXISTS idx_fato_cliente_sk ON fato_pedido(cliente_sk);
CREATE INDEX IF NOT EXISTS idx_fato_produto_sk ON fato_pedido(produto_sk);
CREATE INDEX IF NOT EXISTS idx_fato_tempo_sk ON fato_pedido(tempo_sk);
CREATE INDEX IF NOT EXISTS idx_fato_avaliacao_sk ON fato_pedido(avaliacao_sk);
CREATE INDEX IF NOT EXISTS idx_fato_status_pedido ON fato_pedido(status_pedido);
CREATE INDEX IF NOT EXISTS idx_fato_datas ON fato_pedido(data_aprovacao, data_entrega_cliente);
