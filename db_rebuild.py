#!/usr/bin/env python
"""
DB 재구성 CLI 도구.

Usage:
    python db_rebuild.py rebuild [start_date]  - 전체 DB 재구성 (기본: 20260201)
    python db_rebuild.py status                - 현재 DB 상태 확인
    python db_rebuild.py sync                  - 증분 동기화
"""
import sys
from services.data_sync_service import rebuild_all_data, show_db_status, sync_all


def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "rebuild":
            start_date = sys.argv[2] if len(sys.argv) > 2 else "20260201"
            rebuild_all_data(start_date)
        elif cmd == "status":
            show_db_status()
        elif cmd == "sync":
            sync_all()
        else:
            print(__doc__)
    else:
        print(__doc__)
        show_db_status()


if __name__ == "__main__":
    main()
