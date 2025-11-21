SET search_path = dex_query_v1, public, dex_query, extensions, pg_catalog;

CREATE TABLE balance (
    network VARCHAR(16),
    token_address VARCHAR(64),
    token_account VARCHAR(64),
    wallet VARCHAR(64),
    amount DECIMAL(76,0),
    decimal BIGINT,
    block_number BIGINT,
    version BIGINT,
    updated_at TIMESTAMP,
    UNIQUE (network, token_address, token_account)
) PARTITION BY LIST (network);

CREATE TABLE balance_bsc
    PARTITION OF balance
    FOR VALUES IN ('BSC');

CREATE TABLE balance_solana
    PARTITION OF balance
    FOR VALUES IN ('SOLANA');
