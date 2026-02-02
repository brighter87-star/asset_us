-- Asset Management Database Schema (Overseas Stocks)
-- Database: asset_us
-- Purpose: Daily lot tracking and portfolio analytics for overseas stocks

-- Create database
CREATE DATABASE IF NOT EXISTS asset_us DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE asset_us;

-- ============================================================
-- Table 1: daily_lots
-- Purpose: Store daily net position lots
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_lots (
    lot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '종목코드',
    stock_name VARCHAR(100) COMMENT '종목명',
    crd_class VARCHAR(10) NOT NULL COMMENT '신용구분 (CASH/CREDIT)',
    loan_dt VARCHAR(20) COMMENT '대출일자',
    trade_date DATE NOT NULL COMMENT '거래일자',
    net_quantity INT NOT NULL COMMENT '순매수량',
    avg_purchase_price DECIMAL(15, 4) NOT NULL COMMENT '평균매수가 (소수점)',
    total_cost DECIMAL(20, 4) NOT NULL COMMENT '총매수금액',

    -- Overseas stock specific
    currency VARCHAR(3) COMMENT '통화코드 (USD/HKD/CNY/JPY/VND)',
    exchange_code VARCHAR(4) COMMENT '거래소코드 (NASD/NYSE/AMEX/SEHK/...)',

    -- Metrics (updated daily)
    holding_days INT COMMENT '보유일수',
    current_price DECIMAL(15, 4) COMMENT '현재가',
    unrealized_pnl DECIMAL(20, 4) COMMENT '미실현손익',
    unrealized_return_pct DECIMAL(10, 4) COMMENT '미실현수익률(%)',

    -- Lifecycle
    is_closed BOOLEAN DEFAULT FALSE COMMENT '종료여부',
    closed_date DATE COMMENT '종료일자',
    realized_pnl DECIMAL(20, 4) COMMENT '실현손익',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_daily_lot (stock_code, crd_class, loan_dt, trade_date),
    INDEX idx_stock_code (stock_code),
    INDEX idx_is_closed (is_closed),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='일별 순매수 lot 테이블 (해외주식)';

-- ============================================================
-- Table 2: portfolio_snapshot
-- Purpose: Daily portfolio composition with weights and returns
-- ============================================================
CREATE TABLE IF NOT EXISTS portfolio_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL COMMENT '스냅샷 일자',
    stock_code VARCHAR(20) NOT NULL COMMENT '종목코드',
    stock_name VARCHAR(100) COMMENT '종목명',
    crd_class VARCHAR(10) NOT NULL COMMENT '신용구분',

    -- Overseas stock specific
    currency VARCHAR(3) COMMENT '통화코드',
    exchange_code VARCHAR(4) COMMENT '거래소코드',

    -- Position metrics
    total_quantity INT NOT NULL COMMENT '총보유수량',
    avg_cost_basis DECIMAL(15, 4) COMMENT '평균단가',
    current_price DECIMAL(15, 4) COMMENT '현재가',
    market_value DECIMAL(20, 4) COMMENT '평가금액',
    total_cost DECIMAL(20, 4) COMMENT '총매수금액',

    -- Performance
    unrealized_pnl DECIMAL(20, 4) COMMENT '미실현손익',
    unrealized_return_pct DECIMAL(10, 4) COMMENT '미실현수익률(%)',
    portfolio_weight_pct DECIMAL(10, 4) COMMENT '포트폴리오 비중(%)',

    -- Portfolio total
    total_portfolio_value DECIMAL(20, 4) COMMENT '전체 포트폴리오 가치',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_snapshot (snapshot_date, stock_code, crd_class),
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_stock_code (stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='포트폴리오 스냅샷 테이블 (해외주식)';

-- ============================================================
-- Table 3: account_trade_history
-- Purpose: Trade history synced from KIS API
-- ============================================================
CREATE TABLE IF NOT EXISTS account_trade_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ord_no VARCHAR(50) UNIQUE NOT NULL COMMENT '주문번호',
    stk_cd VARCHAR(20) COMMENT '종목코드',
    stk_nm VARCHAR(100) COMMENT '종목명',
    io_tp_nm VARCHAR(50) COMMENT '매매구분 (매수/매도)',
    crd_class VARCHAR(10) COMMENT '신용구분',
    trade_date DATE COMMENT '거래일자',
    ord_tm CHAR(8) COMMENT '주문시간',
    cntr_qty INT COMMENT '체결수량',
    cntr_uv DECIMAL(15, 4) COMMENT '체결단가 (소수점)',
    loan_dt VARCHAR(20) COMMENT '대출일자',

    -- Overseas stock specific
    currency VARCHAR(3) COMMENT '통화코드',
    exchange_code VARCHAR(4) COMMENT '거래소코드',

    INDEX idx_trade_date (trade_date),
    INDEX idx_stock_code (stk_cd),
    INDEX idx_crd_class (crd_class),
    INDEX idx_composite (stk_cd, crd_class, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='계좌 거래내역 테이블 (해외주식)';

-- ============================================================
-- Table 4: holdings
-- Purpose: Current holdings synced from KIS API
-- ============================================================
CREATE TABLE IF NOT EXISTS holdings (
    snapshot_date DATE COMMENT '스냅샷 일자',
    stk_cd VARCHAR(20) COMMENT '종목코드',
    stk_nm VARCHAR(100) COMMENT '종목명',
    rmnd_qty INT COMMENT '잔고수량',
    avg_prc DECIMAL(15, 4) COMMENT '평균단가 (소수점)',
    cur_prc DECIMAL(15, 4) COMMENT '현재가 (소수점)',
    loan_dt VARCHAR(20) COMMENT '대출일자',
    crd_class VARCHAR(10) COMMENT '신용구분',

    -- Overseas stock specific
    currency VARCHAR(3) COMMENT '통화코드',
    exchange_code VARCHAR(4) COMMENT '거래소코드',

    -- Additional fields from KIS API
    evlt_amt DECIMAL(20, 4) COMMENT '평가금액',
    pl_amt DECIMAL(20, 4) COMMENT '평가손익금액',
    pl_rt DECIMAL(10, 4) COMMENT '평가손익률',
    pur_amt DECIMAL(20, 4) COMMENT '매입금액',

    UNIQUE KEY uk_holding (snapshot_date, stk_cd, loan_dt),
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_stock_code (stk_cd)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='보유종목 테이블 (해외주식)';

-- ============================================================
-- Table 5: account_summary
-- Purpose: Account summary synced from KIS API
-- ============================================================
CREATE TABLE IF NOT EXISTS account_summary (
    snapshot_date DATE PRIMARY KEY COMMENT '스냅샷 일자',
    aset_evlt_amt DECIMAL(20, 4) COMMENT '자산평가금액 (주식)',
    tot_est_amt DECIMAL(20, 4) COMMENT '총평가금액 (주식+예수금)',
    invt_bsamt DECIMAL(20, 4) COMMENT '투자원금',

    -- Overseas stock specific (may hold multiple currencies)
    currency VARCHAR(3) COMMENT '기준 통화'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='계좌요약 테이블 (해외주식)';
