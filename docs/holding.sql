-- 创建主表（模板表，不直接存储数据）
CREATE TABLE dex_query_v1.t_smart_holding (
  id bigserial,
  chain_id bigint NOT NULL,
  wallet_address varchar(255) NOT NULL,
  token_address varchar(255) NOT NULL,
  token_icon TEXT,
  token_name VARCHAR(512),
  amount decimal(50,20) NOT NULL DEFAULT 0,
  value_usd decimal(50,20) NOT NULL DEFAULT 0,
  unrealized_profits decimal(50,20) NOT NULL DEFAULT 0,
  pnl decimal(50,20) NOT NULL DEFAULT 0,
  pnl_percentage decimal(50,20) NOT NULL DEFAULT 0,
  avg_price decimal(50,20) NOT NULL DEFAULT 0,
  current_total_cost decimal(50,20) NOT NULL DEFAULT 0,
  marketcap decimal(50,20) NOT NULL DEFAULT 0,
  is_cleared boolean NOT NULL DEFAULT false,
  is_dev boolean NOT NULL DEFAULT false,
  tags varchar(50)[],
  historical_buy_amount decimal(50,20) NOT NULL DEFAULT 0,
  historical_sell_amount decimal(50,20) NOT NULL DEFAULT 0,
  historical_buy_cost decimal(50,20) NOT NULL DEFAULT 0,
  historical_sell_value decimal(50,20) NOT NULL DEFAULT 0,
  historical_buy_count integer NOT NULL DEFAULT 0,
  historical_sell_count integer NOT NULL DEFAULT 0,
  position_opened_at bigint,
  last_transaction_time bigint,
  updated_at bigint NOT NULL,
  created_at bigint NOT NULL,
  PRIMARY KEY (id, chain_id, wallet_address),
  UNIQUE (wallet_address, token_address, chain_id)
) PARTITION BY LIST (chain_id);

-- 添加列注释
COMMENT ON COLUMN dex_query_v1.t_smart_holding.chain_id IS 'bip0044链ID';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.wallet_address IS '钱包地址';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.token_address IS 'token地址';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.token_icon IS 'token图标url';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.token_name IS 'token名称';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.amount IS '当前持仓数量';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.value_usd IS '当前持仓价值USD';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.unrealized_profits IS '当前持仓未实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.pnl IS '已实现盈亏USD（累加）';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.pnl_percentage IS '已实现盈亏百分比（累加）';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.avg_price IS '平均买入价USD';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.current_total_cost IS '当前持仓总花费成本USD';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.marketcap IS '持仓变动时的市值USD';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.is_cleared IS '是否已清仓';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.is_dev IS '是否是该token dev';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.tags IS 'smart money, sniper...';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_buy_amount IS '历史买入数量';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_sell_amount IS '历史卖出数量';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_buy_cost IS '历史买入成本';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_sell_value IS '历史卖出价值';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_buy_count IS '历史买入次数';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.historical_sell_count IS '历史卖出次数';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.position_opened_at IS '首次持仓时间';
COMMENT ON COLUMN dex_query_v1.t_smart_holding.last_transaction_time IS '最近交易时间';

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
