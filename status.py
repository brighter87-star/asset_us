"""
실시간 포트폴리오 현황 조회 (KIS API 직접 호출)
보유 종목, 수익률, 현금, 총 자산을 실시간으로 표시
"""

import sys
import requests
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.kis_service import KISAPIClient

ET  = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")


def fmt_usd(val, sign=False):
    if val is None:
        return "N/A"
    f = float(val)
    s = f"${abs(f):,.2f}"
    if sign:
        s = ("+" if f >= 0 else "-") + s
    return s


def fmt_pct(val):
    if val is None:
        return "N/A"
    f = float(val)
    return f"{'+'if f>=0 else ''}{f:.2f}%"


def get_cash_balance(client: KISAPIClient):
    """inquire-psamount API로 현금 조회 (ord_psbl_frcr_amt + sll_ruse_psbl_amt)"""
    url = f"{client.base_url}/uapi/overseas-stock/v1/trading/inquire-psamount"
    headers = client._get_headers("TTTS3007R")
    params = {
        "CANO": client.cano,
        "ACNT_PRDT_CD": client.acnt_prdt_cd,
        "OVRS_EXCG_CD": "NASD",
        "OVRS_ORD_UNPR": "100",
        "ITEM_CD": "AAPL",
    }
    client._wait_for_rate_limit()
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    if data.get("rt_cd") != "0":
        raise Exception(f"Cash API error: {data.get('msg1')}")
    output = data.get("output", {})
    avail  = float(output.get("ord_psbl_frcr_amt", 0) or 0)
    sll    = float(output.get("sll_ruse_psbl_amt", 0) or 0)
    return avail, sll


def main():
    now_kst = datetime.now(KST)
    now_et  = datetime.now(ET)

    print("=" * 72)
    print("  포트폴리오 현황  (실시간 KIS API)")
    print(f"  {now_kst.strftime('%Y-%m-%d %H:%M')} KST  |  {now_et.strftime('%Y-%m-%d %H:%M')} ET")
    print("=" * 72)

    client = KISAPIClient()
    # 토큰 조회 (만료된 경우에만 갱신)
    client.get_access_token()

    # ── 1. 보유종목 (NASD 조회 시 전체 미국주식 반환) ────────────────────
    print("\n  보유종목 조회 중...")
    holdings = client.get_holdings(exchange_code="NASD", currency="USD")

    # ── 2. 현금 잔고 ──────────────────────────────────────────────────────
    print("  현금 잔고 조회 중...")
    try:
        cash_avail, cash_sll = get_cash_balance(client)
    except Exception as e:
        print(f"  [WARN] 현금 조회 실패: {e}")
        cash_avail, cash_sll = 0.0, 0.0
    cash_total = cash_avail + cash_sll

    # ── 3. 보유종목 출력 ──────────────────────────────────────────────────
    print()
    if not holdings:
        print("  보유 종목 없음\n")
    else:
        hdr = f"{'종목':<8} {'수량':>5} {'평균단가':>10} {'현재가':>10} {'평가금액':>12} {'손익':>12} {'수익률':>9}"
        print(hdr)
        print("-" * 72)

        total_cost = 0.0
        total_mkt  = 0.0
        total_pnl  = 0.0

        for h in holdings:
            ticker  = h.get("ovrs_pdno", "")
            qty     = int(h.get("ovrs_cblc_qty", 0) or 0)
            avg     = float(h.get("pchs_avg_pric", 0) or 0)
            cur     = float(h.get("now_pric2", 0) or 0)
            mkt_val = float(h.get("ovrs_stck_evlu_amt", 0) or 0)
            pnl     = float(h.get("frcr_evlu_pfls_amt", 0) or 0)
            pnl_pct = float(h.get("evlu_pfls_rt", 0) or 0)
            cost    = float(h.get("frcr_buy_amt_smtl1", 0) or 0)

            total_cost += cost
            total_mkt  += mkt_val
            total_pnl  += pnl

            print(f"{ticker:<8} {qty:>5} "
                  f"${avg:>9,.2f} ${cur:>9,.2f} "
                  f"${mkt_val:>11,.2f} "
                  f"{fmt_usd(pnl, sign=True):>12} "
                  f"{fmt_pct(pnl_pct):>9}")

        print("-" * 72)
        # frcr_buy_amt_smtl1이 0인 경우 평가금액-손익으로 역산
        if total_cost == 0 and total_mkt > 0:
            total_cost = total_mkt - total_pnl
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        print(f"{'합계':<8} {'':>5} {'':>10} {'':>10} "
              f"${total_mkt:>11,.2f} "
              f"{fmt_usd(total_pnl, sign=True):>12} "
              f"{fmt_pct(total_pnl_pct):>9}")

    # ── 4. 계좌 요약 ─────────────────────────────────────────────────────
    total_asset = total_mkt + cash_total

    print()
    print("=" * 72)
    print(f"  {'주식 평가금액:':<20} {fmt_usd(total_mkt):>12}")
    print(f"  {'현금 (사용가능):':<20} {fmt_usd(cash_avail):>12}")
    if cash_sll > 0:
        print(f"  {'현금 (미결제 매도대금):':<20} {fmt_usd(cash_sll):>12}")
    print(f"  {'현금 합계:':<20} {fmt_usd(cash_total):>12}")
    print(f"  {'─' * 36}")
    print(f"  {'총 자산 (USD):':<20} {fmt_usd(total_asset):>12}")
    print(f"  {'투자 원금 (추정):':<20} {fmt_usd(total_cost):>12}")
    print(f"  {'미실현 손익:':<20} {fmt_usd(total_pnl, sign=True):>12}  ({fmt_pct(total_pnl_pct)})")
    print("=" * 72)

    print()
    print("  [원화(KRW) 자산에 대하여]")
    print("  KIS 해외주식 inquire-balance API는 USD 기준으로만 응답합니다.")
    print("  원화 환산 조회는 별도 KIS API (예: 통합잔고조회)를 사용하거나,")
    print("  현재 USD 총자산에 당일 환율을 곱해 추정하는 방식을 씁니다.")
    print()


if __name__ == "__main__":
    main()
