"""
Portfolio service for overseas stock portfolio analytics and snapshots.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pymysql


def create_portfolio_snapshot(
    conn: pymysql.connections.Connection,
    snapshot_date: Optional[date] = None,
) -> int:
    """
    Create a daily portfolio snapshot from holdings.

    Args:
        conn: Database connection
        snapshot_date: Date for the snapshot. If None, uses today.

    Returns:
        Number of snapshot records created
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    # Get total portfolio value from account_summary
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT aset_evlt_amt, tot_est_amt
            FROM account_summary
            WHERE snapshot_date = %s
            """,
            (snapshot_date,),
        )
        summary = cur.fetchone()

    if not summary:
        # Fallback: calculate from holdings
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(evlt_amt), 0) as total
                FROM holdings
                WHERE snapshot_date = %s
                """,
                (snapshot_date,),
            )
            row = cur.fetchone()
            total_portfolio_value = Decimal(str(row[0])) if row else Decimal(0)
    else:
        total_portfolio_value = Decimal(str(summary.get("tot_est_amt") or summary.get("aset_evlt_amt") or 0))

    if total_portfolio_value == 0:
        print(f"Warning: No portfolio value found for {snapshot_date}")
        return 0

    # Get positions from holdings
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                stk_cd as stock_code,
                MAX(stk_nm) as stock_name,
                crd_class,
                MAX(currency) as currency,
                MAX(exchange_code) as exchange_code,
                SUM(rmnd_qty) as total_quantity,
                SUM(rmnd_qty * avg_prc) / SUM(rmnd_qty) as avg_cost_basis,
                MAX(cur_prc) as current_price,
                SUM(pur_amt) as total_cost,
                SUM(pl_amt) as unrealized_pnl
            FROM holdings
            WHERE snapshot_date = %s AND rmnd_qty > 0
            GROUP BY stk_cd, crd_class
            ORDER BY stk_cd
            """,
            (snapshot_date,),
        )
        positions = cur.fetchall()

    if not positions:
        return 0

    # Delete existing snapshot
    with conn.cursor() as cur:
        cur.execute("DELETE FROM portfolio_snapshot WHERE snapshot_date = %s", (snapshot_date,))

    # Insert new snapshot
    insert_sql = """
        INSERT INTO portfolio_snapshot (
            snapshot_date, stock_code, stock_name, crd_class,
            currency, exchange_code,
            total_quantity, avg_cost_basis, current_price,
            market_value, total_cost,
            unrealized_pnl, unrealized_return_pct, portfolio_weight_pct,
            total_portfolio_value
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count = 0
    with conn.cursor() as cur:
        for pos in positions:
            total_qty = pos["total_quantity"]
            avg_cost = Decimal(str(pos["avg_cost_basis"])) if pos["avg_cost_basis"] else Decimal(0)
            current_price = Decimal(str(pos["current_price"])) if pos["current_price"] else Decimal(0)
            total_cost = Decimal(str(pos["total_cost"])) if pos["total_cost"] else Decimal(0)
            unrealized_pnl = Decimal(str(pos["unrealized_pnl"])) if pos["unrealized_pnl"] is not None else Decimal(0)

            market_value = current_price * Decimal(total_qty)
            unrealized_return_pct = (unrealized_pnl / total_cost * 100) if total_cost > 0 else Decimal(0)
            portfolio_weight_pct = (market_value / total_portfolio_value * 100) if total_portfolio_value > 0 else Decimal(0)

            cur.execute(
                insert_sql,
                (
                    snapshot_date,
                    pos["stock_code"],
                    pos["stock_name"],
                    pos["crd_class"],
                    pos.get("currency", "USD"),
                    pos.get("exchange_code", "NASD"),
                    total_qty,
                    float(avg_cost),
                    float(current_price),
                    float(market_value),
                    float(total_cost),
                    float(unrealized_pnl),
                    float(unrealized_return_pct),
                    float(portfolio_weight_pct),
                    float(total_portfolio_value),
                ),
            )
            count += 1

    conn.commit()
    return count


def get_portfolio_composition(
    conn: pymysql.connections.Connection,
    snapshot_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Get current portfolio composition."""
    if snapshot_date is None:
        snapshot_date = date.today()

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                stock_code, stock_name, crd_class,
                currency, exchange_code,
                total_quantity, avg_cost_basis, current_price,
                market_value, total_cost,
                unrealized_pnl, unrealized_return_pct, portfolio_weight_pct,
                total_portfolio_value
            FROM portfolio_snapshot
            WHERE snapshot_date = %s
            ORDER BY portfolio_weight_pct DESC
            """,
            (snapshot_date,),
        )
        return cur.fetchall()


def get_position_summary(
    conn: pymysql.connections.Connection,
    stock_code: str,
) -> Dict[str, Any]:
    """Get detailed summary for a specific position."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) as num_lots,
                stock_name,
                crd_class,
                currency,
                exchange_code,
                MIN(trade_date) as earliest_purchase,
                MAX(trade_date) as latest_purchase,
                SUM(net_quantity) as total_shares,
                SUM(total_cost) as total_cost,
                SUM(total_cost) / SUM(net_quantity) as avg_cost_basis,
                MAX(current_price) as current_price,
                SUM(unrealized_pnl) as total_unrealized_pnl
            FROM daily_lots
            WHERE stock_code = %s AND is_closed = FALSE
            GROUP BY stock_name, crd_class, currency, exchange_code
            """,
            (stock_code,),
        )
        result = cur.fetchone()

        if not result:
            return {}

        total_cost = Decimal(str(result["total_cost"])) if result["total_cost"] else Decimal(0)
        total_pnl = Decimal(str(result["total_unrealized_pnl"])) if result["total_unrealized_pnl"] else Decimal(0)
        return_pct = (total_pnl / total_cost * 100) if total_cost > 0 else Decimal(0)
        result["total_return_pct"] = float(return_pct)

        return result
