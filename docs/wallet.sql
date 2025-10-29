CREATE TABLE dex_query_v1.t_smart_wallet (
  id bigserial PRIMARY KEY,
  wallet_address VARCHAR(512) NOT NULL,
  avatar VARCHAR(512) NOT NULL,
  balance DECIMAL(50,20) NOT NULL DEFAULT 0,
  balance_usd DECIMAL(50,20) NOT NULL DEFAULT 0,
  chain_id BIGINT,
  tags VARCHAR(50)[],
  twitter_name VARCHAR(50),
  twitter_username VARCHAR(50),
  wallet_type INTEGER DEFAULT 0,
  asset_multiple DECIMAL(50,20) NOT NULL DEFAULT 0,
  token_list JSONB,
  avg_cost_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  buy_num_30d INTEGER,
  sell_num_30d INTEGER,
  win_rate_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  avg_cost_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  buy_num_7d INTEGER,
  sell_num_7d INTEGER,
  win_rate_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  avg_cost_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  buy_num_1d INTEGER,
  sell_num_1d INTEGER,
  win_rate_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_pic_30d TEXT,
  unrealized_profit_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  total_cost_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  avg_realized_profit_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  unrealized_profit_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  total_cost_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  avg_realized_profit_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  pnl_percentage_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  unrealized_profit_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  total_cost_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  avg_realized_profit_1d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_gt500_30d INTEGER,
  distribution_200to500_30d INTEGER,
  distribution_0to200_30d INTEGER,
  distribution_n50to0_30d INTEGER,
  distribution_lt50_30d INTEGER,
  distribution_gt500_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_200to500_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_0to200_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_n50to0_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_lt50_percentage_30d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_gt500_7d INTEGER,
  distribution_200to500_7d INTEGER,
  distribution_0to200_7d INTEGER,
  distribution_n50to0_7d INTEGER,
  distribution_lt50_7d INTEGER,
  distribution_gt500_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_200to500_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_0to200_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_n50to0_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  distribution_lt50_percentage_7d DECIMAL(50,20) NOT NULL DEFAULT 0,
  last_transaction_time BIGINT,
  is_active BOOLEAN,
  updated_at bigint NOT NULL,
  created_at bigint NOT NULL,

  -- 唯一索引
  CONSTRAINT unique_wallet_address UNIQUE (wallet_address)
);

-- 添加列注释
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.wallet_address IS '钱包地址';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avatar IS '头像url';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.balance IS '钱包余额native token';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.balance_usd IS '钱包余额USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.chain_id IS 'bip0044链ID';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.tags IS 'smart_money, sniper...';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.twitter_name IS 'twitter name';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.twitter_username IS 'twitter username';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.wallet_type IS '钱包类型';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.asset_multiple IS '盈亏资产倍数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.token_list IS '最近交易过的token(3个)';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_cost_30d IS '30天平均成本';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.buy_num_30d IS '30天买入次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.sell_num_30d IS '30天卖出次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.win_rate_30d IS '30天胜率';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_cost_7d IS '7天平均成本';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.buy_num_7d IS '7天买入次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.sell_num_7d IS '7天卖出次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.win_rate_7d IS '7天胜率';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_cost_1d IS '1天平均成本';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.buy_num_1d IS '1天买入次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.sell_num_1d IS '1天卖出次数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.win_rate_1d IS '1天胜率';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_30d IS '30天已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_percentage_30d IS '30天已实现盈亏百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_pic_30d IS '30天已实现盈亏折线图';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.unrealized_profit_30d IS '30天未实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.total_cost_30d IS '30天总成本USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_realized_profit_30d IS '30天平均已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_7d IS '7天已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_percentage_7d IS '7天已实现盈亏百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.unrealized_profit_7d IS '7天未实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.total_cost_7d IS '7天总成本USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_realized_profit_7d IS '7天平均已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_1d IS '1天已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.pnl_percentage_1d IS '1天已实现盈亏百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.unrealized_profit_1d IS '1天未实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.total_cost_1d IS '1天总成本USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.avg_realized_profit_1d IS '1天平均已实现盈亏USD';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_gt500_30d IS '30天收益大于500%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_200to500_30d IS '30天收益200-500%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_0to200_30d IS '30天收益0-200%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_n50to0_30d IS '30天收益-50%到0%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_lt50_30d IS '30天收益小于-50%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_gt500_percentage_30d IS '30天收益大于500%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_200to500_percentage_30d IS '30天收益200-500%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_0to200_percentage_30d IS '30天收益0-200%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_n50to0_percentage_30d IS '30天收益-50%到0%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_lt50_percentage_30d IS '30天收益小于-50%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_gt500_7d IS '7天收益大于500%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_200to500_7d IS '7天收益200-500%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_0to200_7d IS '7天收益0-200%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_n50to0_7d IS '7天收益-50%到0%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_lt50_7d IS '7天收益小于-50%的笔数';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_gt500_percentage_7d IS '7天收益大于500%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_200to500_percentage_7d IS '7天收益200-500%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_0to200_percentage_7d IS '7天收益0-200%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_n50to0_percentage_7d IS '7天收益-50%到0%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.distribution_lt50_percentage_7d IS '7天收益小于-50%的百分比';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.last_transaction_time IS '最近交易时间';
COMMENT ON COLUMN dex_query_v1.t_smart_wallet.is_active IS '是否活跃';

