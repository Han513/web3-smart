CREATE TABLE balance (
    network VARCHAR(16),
    token_address VARCHAR(64),
    token_account VARCHAR(64),   -- 把 token_account 放在 wallet 前面
    wallet VARCHAR(64),
    amount DECIMAL(76,0),
    decimal BIGINT,
    block_number BIGINT,
    version BIGINT,
    updated_at DATETIME
)
UNIQUE KEY (network, token_address, token_account)
PARTITION BY LIST (network) (
    PARTITION p_bsc VALUES IN ('BSC'),
    PARTITION p_solana VALUES IN ('SOLANA')
)
DISTRIBUTED BY HASH(network, token_address) BUCKETS 32
PROPERTIES (
    "replication_allocation" = "tag.location.default: 2",
    "enable_unique_key_merge_on_write" = "true"
);


