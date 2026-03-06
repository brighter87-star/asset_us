#!/usr/bin/env python3
"""
노트북 자동 실행 스크립트 (거래일에만 실행)

주말 및 미국 공휴일(비거래일)은 자동으로 건너뜁니다.
market_index 테이블에 오늘 데이터가 없으면 비거래일로 판단합니다.
(daily_sync.py [8/8]이 market_index를 동기화하므로 이 스크립트는 그 이후에 실행해야 함)

Usage:
    python cron/run_notebook.py          # 오늘 날짜 기준 실행
    python cron/run_notebook.py --force  # 거래일 여부 무시하고 강제 실행
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import get_connection

NOTEBOOK = PROJECT_ROOT / "notebooks" / "portfolio_analysis.ipynb"


def main():
    parser = argparse.ArgumentParser(description="Run portfolio notebook (trading days only)")
    parser.add_argument("--force", action="store_true", help="거래일 여부 무시하고 강제 실행")
    args = parser.parse_args()

    today_et = datetime.now(ET).date()
    print(f"[run_notebook] {today_et} (ET)")

    if not args.force:
        # 1. 주말 skip
        if today_et.weekday() >= 5:
            print(f"[SKIP] 주말 ({today_et}, weekday={today_et.weekday()}). 노트북 실행 안 함.")
            return

        # 2. 비거래일(공휴일) skip — market_index 기준
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM market_index WHERE index_date = %s",
                    (today_et,)
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()

        if count == 0:
            print(f"[SKIP] 비거래일 ({today_et}). market_index 없음 → 공휴일 또는 데이터 없음.")
            return

    # 3. 노트북 실행
    print(f"[RUN] 노트북 실행 시작...")
    result = subprocess.run(
        [
            sys.executable, "-m", "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute",
            "--inplace",
            "--ExecutePreprocessor.timeout=300",
            str(NOTEBOOK),
        ],
        cwd=str(PROJECT_ROOT),
    )

    if result.returncode == 0:
        print(f"[OK] 노트북 실행 완료. 이미지 저장됨: notebooks/images/")
    else:
        print(f"[ERROR] 노트북 실행 실패 (return code: {result.returncode})")
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
