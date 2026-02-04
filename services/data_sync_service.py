"""
Data synchronization service for overseas stocks.
Syncs data from Korea Investment & Securities API to asset_us database.
"""

from datetime import date
from typing import Optional, List, Dict, Any

import pymysql

from db.connection import get_connection
from services.kis_service import KISAPIClient


# 거래소별 통화 매핑
EXCHANGE_CURRENCY_MAP = {
    "NASD": "USD",
    "NYSE": "USD",
    "AMEX": "USD",
    "NAS": "USD",
    "SEHK": "HKD",
    "SHAA": "CNY",
    "SZAA": "CNY",
    "TKSE": "JPY",
    "HASE": "VND",
    "VNSE": "VND",
}

# 대출유형코드 -> 신용구분 매핑
LOAN_TYPE_TO_CRD_CLASS = {
    "00": "CASH",
    "10": "CASH",
    "": "CASH",
    None: "CASH",
}


def _get_crd_class(loan_type_cd: str) -> str:
    """대출유형코드를 신용구분으로 변환"""
    return LOAN_TYPE_TO_CRD_CLASS.get(loan_type_cd, "CREDIT")


def sync_holdings_from_kis(
    conn: pymysql.connections.Connection,
    client: Optional[KISAPIClient] = None,
    snapshot_date: Optional[date] = None,
) -> int:
    """
    KIS API에서 해외주식 잔고를 조회하여 holdings 테이블에 동기화.

    Args:
        conn: Database connection
        client: KIS API client (optional, creates new one if not provided)
        snapshot_date: Snapshot date (default: today)

    Returns:
        Number of holdings synced
    """
    if client is None:
        client = KISAPIClient()

    if snapshot_date is None:
        snapshot_date = date.today()

    # 모든 거래소에서 잔고 조회
    all_holdings = []
    for exchange_code, currency in EXCHANGE_CURRENCY_MAP.items():
        try:
            holdings = client.get_holdings(exchange_code=exchange_code, currency=currency)
            for h in holdings:
                h["_exchange_code"] = exchange_code
                h["_currency"] = currency
            all_holdings.extend(holdings)
        except Exception as e:
            # 일부 거래소에서 잔고가 없을 수 있음
            if "no data" not in str(e).lower():
                print(f"  Warning: {exchange_code} holdings fetch failed: {e}")
            # no data는 정상 (해당 거래소에 잔고 없음)

    if not all_holdings:
        print("  No holdings found")
        return 0

    # 기존 데이터 삭제 (먼저 commit)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM holdings WHERE snapshot_date = %s", (snapshot_date,))
    conn.commit()

    # 새 데이터 삽입 (ON DUPLICATE KEY UPDATE 사용)
    insert_sql = """
        INSERT INTO holdings (
            snapshot_date, stk_cd, stk_nm, rmnd_qty, avg_prc, cur_prc,
            loan_dt, crd_class, currency, exchange_code,
            evlt_amt, pl_amt, pl_rt, pur_amt
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stk_nm = VALUES(stk_nm),
            rmnd_qty = VALUES(rmnd_qty),
            avg_prc = VALUES(avg_prc),
            cur_prc = VALUES(cur_prc),
            crd_class = VALUES(crd_class),
            currency = VALUES(currency),
            exchange_code = VALUES(exchange_code),
            evlt_amt = VALUES(evlt_amt),
            pl_amt = VALUES(pl_amt),
            pl_rt = VALUES(pl_rt),
            pur_amt = VALUES(pur_amt)
    """

    count = 0
    with conn.cursor() as cur:
        for h in all_holdings:
            # 잔고수량이 0이면 스킵
            qty = int(h.get("ovrs_cblc_qty", 0) or 0)
            if qty == 0:
                continue

            loan_type_cd = h.get("loan_type_cd", "")
            crd_class = _get_crd_class(loan_type_cd)

            cur.execute(
                insert_sql,
                (
                    snapshot_date,
                    h.get("ovrs_pdno", ""),  # 종목코드
                    h.get("ovrs_item_name", ""),  # 종목명
                    qty,  # 잔고수량
                    float(h.get("pchs_avg_pric", 0) or 0),  # 평균단가
                    float(h.get("now_pric2", 0) or 0),  # 현재가
                    "",  # loan_dt (해외주식은 보통 비어있음)
                    crd_class,
                    h.get("_currency", "USD"),
                    h.get("_exchange_code", "NASD"),
                    float(h.get("ovrs_stck_evlu_amt", 0) or 0),  # 평가금액
                    float(h.get("frcr_evlu_pfls_amt", 0) or 0),  # 평가손익
                    float(h.get("evlu_pfls_rt", 0) or 0),  # 평가손익률
                    float(h.get("frcr_pchs_amt1", 0) or 0),  # 매입금액
                ),
            )
            count += 1

    conn.commit()
    return count


def sync_trade_history_from_kis(
    conn: pymysql.connections.Connection,
    client: Optional[KISAPIClient] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """
    KIS API에서 해외주식 체결내역을 조회하여 account_trade_history 테이블에 동기화.

    Args:
        conn: Database connection
        client: KIS API client
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)

    Returns:
        Number of trades synced
    """
    if client is None:
        client = KISAPIClient()

    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    if start_date is None:
        # 기본: 1년 전부터
        start_date = (date.today().replace(year=date.today().year - 1)).strftime("%Y%m%d")

    # 모든 거래소에서 체결내역 조회
    all_trades = []
    for exchange_code in EXCHANGE_CURRENCY_MAP.keys():
        try:
            trades = client.get_trade_history(
                start_date=start_date,
                end_date=end_date,
                exchange_code=exchange_code,
            )
            for t in trades:
                t["_exchange_code"] = exchange_code
            all_trades.extend(trades)
        except Exception as e:
            if "no data" not in str(e).lower():
                print(f"  Warning: {exchange_code} trade history fetch failed: {e}")
            else:
                print(f"  {exchange_code}: no trades found")

    if not all_trades:
        print("  No trades found")
        return 0

    # INSERT IGNORE로 중복 방지
    insert_sql = """
        INSERT IGNORE INTO account_trade_history (
            ord_no, stk_cd, stk_nm, io_tp_nm, crd_class,
            trade_date, ord_tm, cntr_qty, cntr_uv, loan_dt,
            currency, exchange_code
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count = 0
    with conn.cursor() as cur:
        for t in all_trades:
            # 매매구분 변환 (01: 매도, 02: 매수)
            sll_buy = t.get("sll_buy_dvsn_cd", "")
            if sll_buy == "01":
                io_tp_nm = "매도"
            elif sll_buy == "02":
                io_tp_nm = "매수"
            else:
                io_tp_nm = t.get("sll_buy_dvsn_cd_name", "")

            # 체결수량이 0이면 스킵
            qty = int(t.get("ft_ccld_qty", 0) or t.get("ccld_qty", 0) or 0)
            if qty == 0:
                continue

            # 주문번호 생성 (ord_dt + ord_gno_brno + odno)
            ord_no = f"{t.get('ord_dt', '')}-{t.get('ord_gno_brno', '')}-{t.get('odno', '')}"

            # 거래일자 파싱
            ord_dt = t.get("ord_dt", "")
            if ord_dt and len(ord_dt) == 8:
                trade_date = f"{ord_dt[:4]}-{ord_dt[4:6]}-{ord_dt[6:8]}"
            else:
                trade_date = None

            exchange_code = t.get("_exchange_code", "NASD")
            currency = EXCHANGE_CURRENCY_MAP.get(exchange_code, "USD")

            cur.execute(
                insert_sql,
                (
                    ord_no,
                    t.get("pdno", ""),  # 종목코드
                    t.get("prdt_name", ""),  # 종목명
                    io_tp_nm,
                    "CASH",  # 해외주식은 대부분 현금거래
                    trade_date,
                    t.get("ord_tmd", ""),  # 주문시간
                    qty,
                    float(t.get("ft_ccld_unpr3", 0) or t.get("ccld_pric", 0) or 0),  # 체결단가
                    "",  # loan_dt
                    currency,
                    exchange_code,
                ),
            )
            count += cur.rowcount

    conn.commit()
    return count


def sync_account_summary_from_kis(
    conn: pymysql.connections.Connection,
    client: Optional[KISAPIClient] = None,
    snapshot_date: Optional[date] = None,
) -> int:
    """
    KIS API에서 계좌 요약 정보를 동기화.
    (holdings 데이터에서 집계)

    Args:
        conn: Database connection
        client: KIS API client
        snapshot_date: Snapshot date

    Returns:
        1 if synced, 0 otherwise
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    # holdings에서 집계
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(evlt_amt), 0) as total_evlt,
                COALESCE(SUM(pur_amt), 0) as total_pur
            FROM holdings
            WHERE snapshot_date = %s
            """,
            (snapshot_date,),
        )
        row = cur.fetchone()

    if row is None:
        return 0

    total_evlt, total_pur = row

    # REPLACE로 upsert
    with conn.cursor() as cur:
        cur.execute(
            """
            REPLACE INTO account_summary (snapshot_date, aset_evlt_amt, tot_est_amt, invt_bsamt)
            VALUES (%s, %s, %s, %s)
            """,
            (snapshot_date, total_evlt, total_evlt, total_pur),
        )

    conn.commit()
    return 1


def sync_all(
    start_date: Optional[str] = None,
    snapshot_date: Optional[date] = None,
) -> None:
    """
    KIS API에서 모든 데이터를 동기화.

    Args:
        start_date: Trade history start date (YYYYMMDD)
        snapshot_date: Snapshot date for holdings/summary
    """
    conn = get_connection()
    client = KISAPIClient()

    try:
        print("Starting KIS API synchronization...")

        # 1. Trade history sync
        print("\n[1] Syncing trade history...")
        trades_count = sync_trade_history_from_kis(conn, client, start_date)
        print(f"  -> {trades_count} trades synced")

        # 2. Holdings sync
        print("\n[2] Syncing holdings...")
        holdings_count = sync_holdings_from_kis(conn, client, snapshot_date)
        print(f"  -> {holdings_count} holdings synced")

        # 3. Account summary sync
        print("\n[3] Syncing account summary...")
        summary_count = sync_account_summary_from_kis(conn, client, snapshot_date)
        print(f"  -> {summary_count} summary synced")

        print(f"\nTotal synced: {trades_count + holdings_count + summary_count} records")

    except Exception as e:
        print(f"Synchronization failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sync_all()
