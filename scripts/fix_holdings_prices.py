"""
Fix historical holdings prices using yfinance.

The cron was running at 20:00 KST (= 06:00 ET), so cur_prc in holdings
reflects the previous day's closing price instead of the snapshot_date's close.

This script:
1. Fetches correct closing prices from yfinance for all tickers
2. Updates holdings.cur_prc and holdings.evlt_amt
3. Recalculates holdings.pl_amt (unrealized PnL)
4. Regenerates portfolio_snapshot and daily_portfolio_snapshot
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from datetime import date
from db.connection import get_connection
from services.portfolio_service import create_portfolio_snapshot, create_daily_portfolio_snapshot
import yfinance as yf
import pandas as pd

CUTOFF = date(2026, 2, 27)  # 2/27 is already correct
START = date(2026, 2, 4)

conn = get_connection()
cur = conn.cursor()

# 1. Get all (snapshot_date, stk_cd) pairs to fix
cur.execute(
    "SELECT DISTINCT snapshot_date, stk_cd, rmnd_qty, avg_prc, pur_amt "
    "FROM holdings WHERE snapshot_date >= %s AND snapshot_date < %s "
    "ORDER BY snapshot_date, stk_cd",
    (START, CUTOFF)
)
holdings_rows = cur.fetchall()

# Collect unique tickers
tickers = sorted(set(r[1] for r in holdings_rows))
dates = sorted(set(r[0] for r in holdings_rows))

print(f"=== Holdings 가격 보정 ===")
print(f"기간: {dates[0]} ~ {dates[-1]}")
print(f"종목: {len(tickers)}개 {tickers}")
print(f"레코드: {len(holdings_rows)}건\n")

# 2. Fetch closing prices from yfinance (one batch call)
print("[1/4] yfinance에서 종가 다운로드 중...")
start_str = START.strftime("%Y-%m-%d")
end_str = CUTOFF.strftime("%Y-%m-%d")  # yfinance end is exclusive, but we want up to 2/26

df = yf.download(tickers, start=start_str, end=end_str, auto_adjust=True, progress=False)

# Handle single vs multi ticker
if len(tickers) == 1:
    close_df = df[['Close']].copy()
    close_df.columns = [tickers[0]]
else:
    close_df = df['Close'].copy()

close_df.index = close_df.index.date  # Convert to date
print(f"  다운로드 완료: {len(close_df)}일 x {len(close_df.columns)}종목\n")

# 3. Update holdings
print("[2/4] holdings 가격 업데이트 중...")
updated = 0
skipped = 0

for snapshot_dt, stk_cd, qty, avg_prc, pur_amt in holdings_rows:
    if snapshot_dt not in close_df.index:
        skipped += 1
        continue

    if stk_cd not in close_df.columns:
        skipped += 1
        continue

    new_price = close_df.loc[snapshot_dt, stk_cd]
    if pd.isna(new_price):
        skipped += 1
        continue

    new_price = round(float(new_price), 2)
    new_evlt = round(new_price * int(qty), 2)
    new_pl = round(new_evlt - float(pur_amt), 2)
    new_pl_rt = round((new_pl / float(pur_amt)) * 100, 2) if float(pur_amt) > 0 else 0

    cur.execute(
        "UPDATE holdings SET cur_prc = %s, evlt_amt = %s, pl_amt = %s, pl_rt = %s "
        "WHERE snapshot_date = %s AND stk_cd = %s",
        (new_price, new_evlt, new_pl, new_pl_rt, snapshot_dt, stk_cd)
    )
    updated += 1

conn.commit()
print(f"  업데이트: {updated}건, 스킵: {skipped}건\n")

# 4. Regenerate portfolio_snapshot for each date
print("[3/4] portfolio_snapshot 재생성 중...")
for dt in dates:
    count = create_portfolio_snapshot(conn, dt)
    print(f"  {dt}: {count} positions")

# 5. Regenerate daily_portfolio_snapshot for each date
print("\n[4/4] daily_portfolio_snapshot 재생성 중...")
for dt in dates:
    ok = create_daily_portfolio_snapshot(conn, dt)
    print(f"  {dt}: {'created' if ok else 'skipped'}")

conn.close()
print("\n=== 완료 ===")
