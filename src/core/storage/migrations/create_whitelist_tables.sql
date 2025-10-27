-- Migration: Create Whitelist System Tables
-- Description: Tables for pool and token whitelist management matching existing network_{chain_id}_dex_pools structure
-- Date: 2025-09-16

-- ============================================
-- 1. DEX Pools Table (matching existing structure)
-- ============================================
-- This matches the existing network_{chain_id}_dex_pools structure
-- We'll create one per chain as needed
CREATE TABLE IF NOT EXISTS network_1_dex_pools_cryo (
    -- Core fields matching existing structure
    address TEXT PRIMARY KEY,
    factory TEXT NOT NULL,
    asset0 TEXT NOT NULL,  -- token0
    asset1 TEXT NOT NULL,  -- token1
    asset2 TEXT,  -- For multi-asset pools (Curve, etc)
    asset3 TEXT,  -- For multi-asset pools
    creation_block INTEGER,
    fee INTEGER,
    tick_spacing INTEGER,
    additional_data JSONB  -- For protocol-specific data like 'stable' flag in Aerodrome
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_network_1_dex_pools_cryo_tokens ON network_1_dex_pools_cryo(asset0, asset1);
CREATE INDEX IF NOT EXISTS idx_network_1_dex_pools_cryo_factory ON network_1_dex_pools_cryo(factory);
CREATE INDEX IF NOT EXISTS idx_network_1_dex_pools_cryo_creation ON network_1_dex_pools_cryo(creation_block);

-- Same structure for other chains (Arbitrum)
CREATE TABLE IF NOT EXISTS network_42161_dex_pools_cryo (
    address TEXT PRIMARY KEY,
    factory TEXT NOT NULL,
    asset0 TEXT NOT NULL,
    asset1 TEXT NOT NULL,
    asset2 TEXT,
    asset3 TEXT,
    creation_block INTEGER,
    fee INTEGER,
    tick_spacing INTEGER,
    additional_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_network_42161_dex_pools_cryo_tokens ON network_42161_dex_pools_cryo(asset0, asset1);
CREATE INDEX IF NOT EXISTS idx_network_42161_dex_pools_cryo_factory ON network_42161_dex_pools_cryo(factory);
CREATE INDEX IF NOT EXISTS idx_network_42161_dex_pools_cryo_creation ON network_42161_dex_pools_cryo(creation_block);

-- Same structure for Base
CREATE TABLE IF NOT EXISTS network_8453_dex_pools_cryo (
    address TEXT PRIMARY KEY,
    factory TEXT NOT NULL,
    asset0 TEXT NOT NULL,
    asset1 TEXT NOT NULL,
    asset2 TEXT,
    asset3 TEXT,
    creation_block INTEGER,
    fee INTEGER,
    tick_spacing INTEGER,
    additional_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_network_8453_dex_pools_cryo_tokens ON network_8453_dex_pools_cryo(asset0, asset1);
CREATE INDEX IF NOT EXISTS idx_network_8453_dex_pools_cryo_factory ON network_8453_dex_pools_cryo(factory);
CREATE INDEX IF NOT EXISTS idx_network_8453_dex_pools_cryo_creation ON network_8453_dex_pools_cryo(creation_block);

-- ============================================
-- 2. Whitelisted Pools Table
-- ============================================
CREATE TABLE IF NOT EXISTS whitelisted_pools_cryo (
    pool_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,

    -- Reference to pool data
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    fee INTEGER,

    -- Liquidity metrics
    liquidity_usd DECIMAL(20, 2),
    volume_24h_usd DECIMAL(20, 2),
    max_slippage_1k DECIMAL(5, 4), -- 5% max = 0.0500

    -- Whitelist metadata
    trusted_token TEXT, -- Which trusted token qualified this pool
    filter_pass_type TEXT CHECK (filter_pass_type IN ('TRUSTED', 'NETWORK', 'MANUAL')),
    iteration_depth INTEGER DEFAULT 0, -- 0 for trusted, 1+ for network effect

    -- Timestamps
    whitelisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_validated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_liquidity_update TIMESTAMP,

    PRIMARY KEY (pool_address, chain_id)
);

CREATE INDEX idx_wp_cryo_chain ON whitelisted_pools_cryo(chain_id);
CREATE INDEX idx_wp_cryo_tokens ON whitelisted_pools_cryo(token0, token1);
CREATE INDEX idx_wp_cryo_liquidity ON whitelisted_pools_cryo(liquidity_usd DESC);
CREATE INDEX idx_wp_cryo_trusted_token ON whitelisted_pools_cryo(trusted_token);
CREATE INDEX idx_wp_cryo_filter_type ON whitelisted_pools_cryo(filter_pass_type);

-- ============================================
-- 3. Token Whitelist Table
-- ============================================
CREATE TABLE IF NOT EXISTS token_whitelist_cryo (
    token_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,

    -- Token metadata
    symbol VARCHAR(32),
    name VARCHAR(255),
    decimals INTEGER,

    -- External references
    coingecko_id VARCHAR(255),

    -- Trust and validation
    is_trusted_token BOOLEAN DEFAULT FALSE, -- USDC, USDT, WETH, etc.
    trust_score INTEGER DEFAULT 0 CHECK (trust_score >= 0 AND trust_score <= 100),

    -- Pool associations
    pool_count INTEGER DEFAULT 0,
    total_liquidity_usd DECIMAL(20, 2),

    -- Timestamps
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (token_address, chain_id)
);

CREATE INDEX idx_tw_cryo_chain ON token_whitelist_cryo(chain_id);
CREATE INDEX idx_tw_cryo_symbol ON token_whitelist_cryo(symbol);
CREATE INDEX idx_tw_cryo_trusted ON token_whitelist_cryo(chain_id, is_trusted_token) WHERE is_trusted_token = TRUE;

-- ============================================
-- 4. Processing Checkpoints Table
-- ============================================
CREATE TABLE IF NOT EXISTS processing_checkpoints_cryo (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    factory_address TEXT NOT NULL,  -- Track by factory
    process_type TEXT NOT NULL, -- 'HISTORICAL_FETCH', 'POOL_PROCESSING', 'FILTER_PASS'

    -- Progress tracking
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,
    current_block BIGINT NOT NULL,

    -- Metrics
    pools_found INTEGER DEFAULT 0,
    pools_processed INTEGER DEFAULT 0,
    pools_whitelisted INTEGER DEFAULT 0,

    -- Status
    status TEXT DEFAULT 'IN_PROGRESS' CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED')),
    error_message TEXT,

    -- Timestamps
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,

    UNIQUE (chain_id, factory_address, process_type, start_block)
);

CREATE INDEX idx_checkpoint_cryo_status ON processing_checkpoints_cryo(status, chain_id);

-- ============================================
-- 5. Liquidity Snapshots Table
-- ============================================
CREATE TABLE IF NOT EXISTS liquidity_snapshots_cryo (
    id SERIAL PRIMARY KEY,
    pool_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,

    -- Snapshot data
    block_number BIGINT NOT NULL,
    snapshot_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Liquidity metrics
    reserve0 TEXT, -- Store as string for precision
    reserve1 TEXT,
    liquidity_usd DECIMAL(20, 2),

    -- Slippage calculations for different trade sizes
    slippage_1k_usd DECIMAL(5, 4),
    slippage_10k_usd DECIMAL(5, 4)
);

CREATE INDEX idx_ls_cryo_pool ON liquidity_snapshots_cryo(pool_address, chain_id);
CREATE INDEX idx_ls_cryo_block ON liquidity_snapshots_cryo(chain_id, block_number DESC);

-- ============================================
-- 6. Trusted Tokens Configuration
-- ============================================
CREATE TABLE IF NOT EXISTS trusted_tokens_cryo (
    token_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,

    PRIMARY KEY (token_address, chain_id)
);

-- Insert default trusted tokens for Ethereum (chain_id = 1)
INSERT INTO trusted_tokens_cryo (token_address, chain_id, symbol, name) VALUES
    (LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), 1, 'USDC', 'USD Coin'),
    (LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7'), 1, 'USDT', 'Tether USD'),
    (LOWER('0xC02aaA39b223FE8D0A0e5C4f27eAD9083C753Ca2'), 1, 'WETH', 'Wrapped Ether'),
    (LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'), 1, 'WBTC', 'Wrapped BTC'),
    (LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F'), 1, 'DAI', 'Dai Stablecoin')
ON CONFLICT DO NOTHING;

-- Insert trusted tokens for Arbitrum (chain_id = 42161)
INSERT INTO trusted_tokens_cryo (token_address, chain_id, symbol, name) VALUES
    (LOWER('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8'), 42161, 'USDC.e', 'Bridged USDC'),
    (LOWER('0xaf88d065e77c8cC2239327C5EDb3A432268e5831'), 42161, 'USDC', 'USD Coin'),
    (LOWER('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'), 42161, 'USDT', 'Tether USD'),
    (LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'), 42161, 'WETH', 'Wrapped Ether'),
    (LOWER('0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f'), 42161, 'WBTC', 'Wrapped BTC'),
    (LOWER('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1'), 42161, 'DAI', 'Dai Stablecoin')
ON CONFLICT DO NOTHING;

-- Insert trusted tokens for Base (chain_id = 8453)
INSERT INTO trusted_tokens_cryo (token_address, chain_id, symbol, name) VALUES
    (LOWER('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'), 8453, 'USDC', 'USD Coin'),
    (LOWER('0x4200000000000000000000000000000000000006'), 8453, 'WETH', 'Wrapped Ether'),
    (LOWER('0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb'), 8453, 'DAI', 'Dai Stablecoin')
ON CONFLICT DO NOTHING;

-- ============================================
-- Helper Views
-- ============================================

-- Join pools with whitelist data
CREATE OR REPLACE VIEW v_pool_whitelist_status_cryo AS
SELECT
    p.address,
    p.factory,
    p.asset0,
    p.asset1,
    p.fee,
    p.additional_data,
    p.creation_block,
    wp.pool_address IS NOT NULL as is_whitelisted,
    wp.liquidity_usd,
    wp.max_slippage_1k,
    wp.filter_pass_type,
    wp.trusted_token,
    wp.whitelisted_at
FROM network_1_dex_pools_cryo p
LEFT JOIN whitelisted_pools_cryo wp ON p.address = wp.pool_address AND wp.chain_id = 1;

-- Tokens with their pool counts
CREATE OR REPLACE VIEW v_token_statistics_cryo AS
SELECT
    tw.token_address,
    tw.chain_id,
    tw.symbol,
    tw.name,
    tw.is_trusted_token,
    COUNT(DISTINCT wp.pool_address) as whitelisted_pool_count,
    SUM(wp.liquidity_usd) as total_liquidity
FROM token_whitelist_cryo tw
LEFT JOIN whitelisted_pools_cryo wp ON
    tw.chain_id = wp.chain_id AND
    (tw.token_address = wp.token0 OR tw.token_address = wp.token1)
GROUP BY tw.token_address, tw.chain_id, tw.symbol, tw.name, tw.is_trusted_token;

-- ============================================
-- Update Triggers
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_checkpoints_cryo_updated
BEFORE UPDATE ON processing_checkpoints_cryo
FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_token_whitelist_cryo_updated
BEFORE UPDATE ON token_whitelist_cryo
FOR EACH ROW EXECUTE FUNCTION update_updated_at();