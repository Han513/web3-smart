-- 创建主表（模板表，不直接存储数据）
CREATE TABLE dex_query_v1.t_smart_holding (
  id serial,
  chain_id bigint NOT NULL,
  wallet_address varchar(255) NOT NULL,
  token_address varchar(255) NOT NULL,
  token_icon varchar(255),
  token_name varchar(255),
  amount decimal(36,18) NOT NULL DEFAULT 0,
  value_usd decimal(36,18) NOT NULL DEFAULT 0,
  unrealized_profits decimal(36,18) NOT NULL DEFAULT 0,
  pnl decimal(36,18) NOT NULL DEFAULT 0,
  pnl_percentage decimal(10,4) NOT NULL DEFAULT 0,
  avg_price decimal(36,18) NOT NULL DEFAULT 0,
  current_total_cost decimal(36,18) NOT NULL DEFAULT 0,
  marketcap decimal(36,18) NOT NULL DEFAULT 0,
  is_cleared boolean NOT NULL DEFAULT false,
  is_dev boolean NOT NULL DEFAULT false,
  tags varchar(50)[],
  historical_buy_amount decimal(36,18) NOT NULL DEFAULT 0,
  historical_sell_amount decimal(36,18) NOT NULL DEFAULT 0,
  historical_buy_cost decimal(36,18) NOT NULL DEFAULT 0,
  historical_sell_value decimal(36,18) NOT NULL DEFAULT 0,
  historical_buy_count integer NOT NULL DEFAULT 0,
  historical_sell_count integer NOT NULL DEFAULT 0,
  position_opened_at bigint,
  last_transaction_time bigint,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id, chain_id, wallet_address),
  UNIQUE (wallet_address, token_address, chain_id, is_dev)
) PARTITION BY LIST (chain_id);

-- 为 chain_id = 501 创建一级分区
CREATE TABLE t_smart_holding_501 PARTITION OF dex_query_v1.t_smart_holding
    FOR VALUES IN (501)
    PARTITION BY HASH (wallet_address);

-- 为 chain_id = 9006 创建一级分区
CREATE TABLE t_smart_holding_9006 PARTITION OF dex_query_v1.t_smart_holding
    FOR VALUES IN (9006)
    PARTITION BY HASH (wallet_address);

-- 为每个一级分区创建 50 个哈希子分区（示例：chain_id=501）
DO $$
BEGIN
FOR i IN 0..49 LOOP
        EXECUTE format(
            'CREATE TABLE t_smart_holding_501_%s PARTITION OF t_smart_holding_501 FOR VALUES WITH (MODULUS 50, REMAINDER %s)',
            i, i
        );
EXECUTE format('CREATE INDEX ON t_smart_holding_501_%s (token_address, is_dev)', i);
EXECUTE format('CREATE INDEX ON t_smart_holding_501_%s (last_transaction_time)', i);
END LOOP;
END $$;

-- 为 chain_id=9006 创建 50 个哈希子分区
DO $$
BEGIN
FOR i IN 0..49 LOOP
        EXECUTE format(
            'CREATE TABLE t_smart_holding_9006_%s PARTITION OF t_smart_holding_9006 FOR VALUES WITH (MODULUS 50, REMAINDER %s)',
            i, i
        );
EXECUTE format('CREATE INDEX ON t_smart_holding_9006_%s (token_address, is_dev)', i);
EXECUTE format('CREATE INDEX ON t_smart_holding_9006_%s (last_transaction_time)', i);
END LOOP;
END $$;

-- 创建复合索引
CREATE INDEX ON dex_query_v1.t_smart_holding (token_address, is_dev);
CREATE INDEX ON dex_query_v1.t_smart_holding (last_transaction_time);
