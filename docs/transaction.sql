-- 创建主表
CREATE TABLE dex_query_v1.t_smart_transaction (
  id bigserial,
  wallet_address VARCHAR(100) NOT NULL,
  wallet_balance DECIMAL(50,20) NOT NULL DEFAULT 0,
  token_address VARCHAR(100) NOT NULL,
  token_icon TEXT,
  token_name VARCHAR(512),
  price DECIMAL(50,20) NOT NULL DEFAULT 0,
  amount DECIMAL(50,20) NOT NULL DEFAULT 0,
  marketcap DECIMAL(50,20) NOT NULL DEFAULT 0,
  value DECIMAL(50,20) NOT NULL DEFAULT 0,
  holding_percentage DECIMAL(50,20) NOT NULL DEFAULT 0,
  chain_id BIGINT NOT NULL DEFAULT 501,
  realized_profit DECIMAL(50,20) NOT NULL DEFAULT 0,
  realized_profit_percentage DECIMAL(50,20) NOT NULL DEFAULT 0,
  transaction_type VARCHAR(10) NOT NULL,
  transaction_time BIGINT NOT NULL,
  signature VARCHAR(100) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  from_token_address VARCHAR(100) NOT NULL,
  from_token_symbol VARCHAR(512),
  from_token_amount DECIMAL(50,20) NOT NULL DEFAULT 0,
  dest_token_address VARCHAR(100) NOT NULL,
  dest_token_symbol VARCHAR(512),
  dest_token_amount DECIMAL(50,20) NOT NULL DEFAULT 0,
  created_at bigint NOT NULL,
  PRIMARY KEY (id, chain_id, wallet_address)
) PARTITION BY LIST (chain_id);

-- 添加列注释
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.wallet_address IS '钱包地址';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.wallet_balance IS '钱包余额';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.token_address IS 'token地址';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.token_icon IS 'token图标url';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.token_name IS 'token名称';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.price IS '当前交易token价格USD';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.amount IS 'token数量';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.marketcap IS '该笔交易时的市值USD';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.value IS 'token amount * price';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.holding_percentage IS '当前持仓百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.chain_id IS 'bip0044链ID';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.realized_profit IS '已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.realized_profit_percentage IS '已实现盈亏百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.transaction_type IS '交易类型';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.transaction_time IS '交易时间';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.signature IS '交易hash';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.from_token_address IS 'token in address';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.from_token_symbol IS 'token in symbol';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.from_token_amount IS 'token in amount';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.dest_token_address IS 'token out address';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.dest_token_symbol IS 'token out symbol';
COMMENT ON COLUMN dex_query_v1.t_smart_transaction.dest_token_amount IS 'token out amount';

-- 创建常用 chain_id 的一级分区
CREATE TABLE t_smart_transaction_chain_501 PARTITION OF dex_query_v1.t_smart_transaction
    FOR VALUES IN (501) PARTITION BY HASH (wallet_address);

CREATE TABLE t_smart_transaction_chain_9006 PARTITION OF dex_query_v1.t_smart_transaction
    FOR VALUES IN (9006) PARTITION BY HASH (wallet_address);

-- 使用循环为每个 chain_id 创建 5 个哈希分区
DO $$
DECLARE
chain_ids INT[] := ARRAY[501, 9006]; -- 更新为支持 501 和 9006
    i INT;
    chain_id INT;
BEGIN
    FOREACH chain_id IN ARRAY chain_ids LOOP
        FOR i IN 0..4 LOOP
            EXECUTE format(
                'CREATE TABLE t_smart_transaction_chain_%s_bucket_%s PARTITION OF t_smart_transaction_chain_%s '
                'FOR VALUES WITH (MODULUS 5, REMAINDER %s)',
                chain_id, i, chain_id, i
            );
END LOOP;
END LOOP;
END $$;

-- 创建索引
-- 1. 唯一索引（包含分区键）
CREATE UNIQUE INDEX unique_transaction ON dex_query_v1.t_smart_transaction
    (wallet_address, token_address, signature, log_index, transaction_time, chain_id);

-- 2. TokenAddress 查询优化索引
CREATE INDEX idx_token_address ON dex_query_v1.t_smart_transaction (token_address);

-- 3. TransactionTime 查询优化索引
CREATE INDEX idx_transaction_time ON dex_query_v1.t_smart_transaction (transaction_time);
