-- Market Index table for US market indices (S&P 500, NASDAQ)
-- Used for comparing portfolio performance against market benchmarks

USE asset_us;

CREATE TABLE IF NOT EXISTS market_index (
    index_date DATE PRIMARY KEY COMMENT '지수 일자',

    -- S&P 500 (SPX)
    sp500_close DECIMAL(10, 2) COMMENT 'S&P 500 종가',
    sp500_change DECIMAL(10, 2) COMMENT 'S&P 500 전일대비',
    sp500_change_pct DECIMAL(10, 4) COMMENT 'S&P 500 등락률(%)',

    -- NASDAQ Composite (IXIC)
    nasdaq_close DECIMAL(12, 2) COMMENT 'NASDAQ 종가',
    nasdaq_change DECIMAL(10, 2) COMMENT 'NASDAQ 전일대비',
    nasdaq_change_pct DECIMAL(10, 4) COMMENT 'NASDAQ 등락률(%)',

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_index_date (index_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='시장지수(S&P500/NASDAQ) 일별 데이터';
