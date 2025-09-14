-- 创建主表
CREATE TABLE dex_query_v1.t_smart_transaction (
  id SERIAL,
  wallet_address VARCHAR(100) NOT NULL,
  wallet_balance DECIMAL(36,18) NOT NULL,
  token_address VARCHAR(100) NOT NULL,
  token_icon TEXT,
  token_name VARCHAR(100),
  price DECIMAL(36,18),
  amount DECIMAL(36,18) NOT NULL,
  marketcap DECIMAL(36,18),
  value DECIMAL(36,18),
  holding_percentage DECIMAL(10,4),
  chain_id INT NOT NULL DEFAULT 501,
  realized_profit DECIMAL(36,18),
  realized_profit_percentage DECIMAL(10,4),
  transaction_type VARCHAR(10) NOT NULL,
  transaction_time BIGINT NOT NULL,
  signature VARCHAR(100) NOT NULL,
  from_token_address VARCHAR(100) NOT NULL,
  from_token_symbol VARCHAR(100),
  from_token_amount DECIMAL(36,18) NOT NULL,
  dest_token_address VARCHAR(100) NOT NULL,
  dest_token_symbol VARCHAR(100),
  dest_token_amount DECIMAL(36,18) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id, chain_id, wallet_address)
) PARTITION BY LIST (chain_id);

-- 创建常用 chain_id 的一级分区
CREATE TABLE t_smart_transaction_chain_501 PARTITION OF dex_query_v1.t_smart_transaction
    FOR VALUES IN (501) PARTITION BY HASH (wallet_address);

CREATE TABLE t_smart_transaction_chain_9006 PARTITION OF dex_query_v1.t_smart_transaction
    FOR VALUES IN (9006) PARTITION BY HASH (wallet_address);

-- 使用循环为每个 chain_id 创建 20 个哈希分区
DO $$
DECLARE
chain_ids INT[] := ARRAY[501, 9006]; -- 更新为支持 501 和 9006
    i INT;
    chain_id INT;
BEGIN
    FOREACH chain_id IN ARRAY chain_ids LOOP
        FOR i IN 0..19 LOOP
            EXECUTE format(
                'CREATE TABLE t_smart_transaction_chain_%s_bucket_%s PARTITION OF t_smart_transaction_chain_%s '
                'FOR VALUES WITH (MODULUS 20, REMAINDER %s)',
                chain_id, i, chain_id, i
            );
END LOOP;
END LOOP;
END $$;

-- 创建索引
-- 1. 唯一索引（包含分区键）
CREATE UNIQUE INDEX unique_transaction ON dex_query_v1.t_smart_transaction
    (wallet_address, token_address, signature, transaction_time, chain_id);

-- 2. TokenAddress 查询优化索引
CREATE INDEX idx_token_address ON dex_query_v1.t_smart_transaction (token_address);

-- 3. TransactionTime 查询优化索引
CREATE INDEX idx_transaction_time ON dex_query_v1.t_smart_transaction (transaction_time);
