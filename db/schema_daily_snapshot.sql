-- Daily portfolio snapshot table for US stocks
-- Records daily asset status including holdings and cash flows
-- Used for calculating time-weighted and money-weighted returns

USE asset_us;

CREATE TABLE IF NOT EXISTS daily_portfolio_snapshot (
    snapshot_date DATE PRIMARY KEY COMMENT '스냅샷 일자',

    -- Total assets (in USD)
    total_asset_usd DECIMAL(20, 4) COMMENT '총자산 (USD)',
    stock_value_usd DECIMAL(20, 4) COMMENT '주식평가금액 (USD)',
    total_cost_usd DECIMAL(20, 4) COMMENT '총매입금액 (USD)',

    -- Daily cash flows (in USD)
    deposit_usd DECIMAL(20, 4) COMMENT '당일 입금액 (USD)',
    withdraw_usd DECIMAL(20, 4) COMMENT '당일 출금액 (USD)',

    -- Daily transactions (in USD)
    buy_amt_usd DECIMAL(20, 4) COMMENT '당일 매수금액 (USD)',
    sell_amt_usd DECIMAL(20, 4) COMMENT '당일 매도금액 (USD)',

    -- Performance
    unrealized_pnl_usd DECIMAL(20, 4) COMMENT '미실현손익 (USD)',
    realized_pnl_usd DECIMAL(20, 4) COMMENT '실현손익 (USD)',

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_snapshot_date (snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='일자별 포트폴리오 스냅샷 (해외주식)';
