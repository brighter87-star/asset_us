"""
Lot service for daily net lot construction and management (overseas stocks).
"""

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pymysql


def _is_buy(io_tp_nm: Optional[str]) -> bool:
    """Check if trade is a buy."""
    if not io_tp_nm:
        return False
    return "매수" in io_tp_nm


def _is_sell(io_tp_nm: Optional[str]) -> bool:
    """Check if trade is a sell."""
    if not io_tp_nm:
        return False
    return "매도" in io_tp_nm and "매수" not in io_tp_nm


def construct_daily_lots(
    conn: pymysql.connections.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """
    Construct daily lots from trade history.

    Args:
        conn: Database connection
        start_date: Start date (YYYY-MM-DD). If None, defaults to earliest trade.
        end_date: End date (YYYY-MM-DD). If None, processes up to today.
    """
    where_clauses = []
    params: Dict[str, Any] = {}

    if start_date:
        where_clauses.append("trade_date >= %(start_date)s")
        params["start_date"] = start_date

    if end_date:
        where_clauses.append("trade_date <= %(end_date)s")
        params["end_date"] = end_date

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                stk_cd,
                stk_nm,
                io_tp_nm,
                crd_class,
                trade_date,
                cntr_qty,
                cntr_uv,
                loan_dt,
                currency,
                exchange_code
            FROM account_trade_history
            {where_sql}
            ORDER BY trade_date ASC, stk_cd, crd_class, loan_dt
            """,
            params,
        )

        trades = cur.fetchall()

    if not trades:
        print("No trades found for lot construction")
        return

    # Group trades by (stock_code, crd_class, loan_dt, trade_date)
    grouped: Dict[Tuple[str, str, str, date], List[Dict]] = {}

    for trade in trades:
        key = (
            trade["stk_cd"],
            trade["crd_class"] or "CASH",
            trade["loan_dt"] or "",
            trade["trade_date"],
        )
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(trade)

    # Process each group
    for (stock_code, crd_class, loan_dt, trade_date), group in grouped.items():
        buys = [t for t in group if _is_buy(t["io_tp_nm"])]
        sells = [t for t in group if _is_sell(t["io_tp_nm"])]

        buy_qty = sum(t["cntr_qty"] or 0 for t in buys)
        sell_qty = sum(t["cntr_qty"] or 0 for t in sells)

        stock_name = group[0]["stk_nm"]
        currency = group[0].get("currency", "USD")
        exchange_code = group[0].get("exchange_code", "NASD")

        # Check existing open lots
        existing_qty = _get_existing_lot_quantity(conn, stock_code, crd_class, loan_dt, trade_date)

        if existing_qty > 0 and sell_qty > 0:
            close_qty = min(sell_qty, existing_qty)

            total_sell_value = sum(
                Decimal(str(t["cntr_qty"] or 0)) * Decimal(str(t["cntr_uv"] or 0))
                for t in sells
            )
            avg_sell_price = total_sell_value / Decimal(sell_qty) if sell_qty > 0 else Decimal(0)

            _reduce_lots_lifo(conn, stock_code, crd_class, loan_dt, close_qty, trade_date, avg_sell_price)

            remaining_sell = sell_qty - close_qty
            net_buy = buy_qty - remaining_sell
        else:
            net_buy = buy_qty - sell_qty

        # Create new lot if net buy
        if net_buy > 0:
            total_buy_value = sum(
                Decimal(str(t["cntr_qty"] or 0)) * Decimal(str(t["cntr_uv"] or 0))
                for t in buys
            )
            avg_price = total_buy_value / Decimal(buy_qty) if buy_qty > 0 else Decimal(0)
            total_cost = avg_price * Decimal(net_buy)

            _insert_daily_lot(
                conn,
                stock_code,
                stock_name,
                crd_class,
                loan_dt,
                trade_date,
                net_buy,
                avg_price,
                total_cost,
                currency,
                exchange_code,
            )
        elif net_buy < 0 and existing_qty == 0:
            print(f"Warning: Sold {abs(net_buy)} of {stock_code} without matching lots")

    conn.commit()


def _get_existing_lot_quantity(
    conn: pymysql.connections.Connection,
    stock_code: str,
    crd_class: str,
    loan_dt: str,
    before_date: date,
) -> int:
    """Get total quantity of existing open lots before a given date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(net_quantity), 0)
            FROM daily_lots
            WHERE stock_code = %s
              AND crd_class = %s
              AND (loan_dt = %s OR (loan_dt = '' AND %s = ''))
              AND is_closed = FALSE
              AND trade_date < %s
            """,
            (stock_code, crd_class, loan_dt or '', loan_dt or '', before_date),
        )
        return cur.fetchone()[0]


def _insert_daily_lot(
    conn: pymysql.connections.Connection,
    stock_code: str,
    stock_name: str,
    crd_class: str,
    loan_dt: str,
    trade_date: date,
    net_quantity: int,
    avg_purchase_price: Decimal,
    total_cost: Decimal,
    currency: str = "USD",
    exchange_code: str = "NASD",
) -> None:
    """Insert or update a daily lot."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_lots (
                stock_code, stock_name, crd_class, loan_dt, trade_date,
                net_quantity, avg_purchase_price, total_cost,
                currency, exchange_code
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                net_quantity = VALUES(net_quantity),
                avg_purchase_price = VALUES(avg_purchase_price),
                total_cost = VALUES(total_cost),
                currency = VALUES(currency),
                exchange_code = VALUES(exchange_code),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                stock_code,
                stock_name,
                crd_class,
                loan_dt or '',
                trade_date,
                net_quantity,
                float(avg_purchase_price),
                float(total_cost),
                currency,
                exchange_code,
            ),
        )


def _reduce_lots_lifo(
    conn: pymysql.connections.Connection,
    stock_code: str,
    crd_class: str,
    loan_dt: str,
    sell_qty: int,
    sell_date: date,
    sell_price: Decimal = Decimal(0),
) -> None:
    """Reduce existing lots using LIFO (Last In First Out)."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT lot_id, net_quantity, trade_date, avg_purchase_price, total_cost
            FROM daily_lots
            WHERE stock_code = %s
              AND crd_class = %s
              AND loan_dt = %s
              AND is_closed = FALSE
              AND trade_date <= %s
            ORDER BY trade_date DESC
            """,
            (stock_code, crd_class, loan_dt or '', sell_date),
        )
        lots = cur.fetchall()

    remaining = sell_qty

    with conn.cursor() as cur:
        for lot in lots:
            if remaining <= 0:
                break

            lot_id = lot["lot_id"]
            lot_qty = lot["net_quantity"]
            lot_trade_date = lot["trade_date"]
            lot_avg_price = Decimal(str(lot["avg_purchase_price"])) if lot["avg_purchase_price"] else Decimal(0)

            if isinstance(lot_trade_date, date):
                holding_days = (sell_date - lot_trade_date).days
            else:
                holding_days = 0

            if lot_qty <= remaining:
                realized_pnl = (sell_price - lot_avg_price) * Decimal(lot_qty)

                cur.execute(
                    """
                    UPDATE daily_lots
                    SET is_closed = TRUE,
                        closed_date = %s,
                        net_quantity = 0,
                        current_price = %s,
                        holding_days = %s,
                        realized_pnl = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE lot_id = %s
                    """,
                    (sell_date, float(sell_price), holding_days, float(realized_pnl), lot_id),
                )
                remaining -= lot_qty
            else:
                new_qty = lot_qty - remaining
                new_total_cost = lot_avg_price * Decimal(new_qty)

                cur.execute(
                    """
                    UPDATE daily_lots
                    SET net_quantity = %s,
                        total_cost = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE lot_id = %s
                    """,
                    (new_qty, float(new_total_cost), lot_id),
                )
                remaining = 0

    if remaining > 0:
        print(f"Warning: Sold {remaining} of {stock_code} without matching lots")


def update_lot_metrics(conn: pymysql.connections.Connection, today: Optional[date] = None) -> int:
    """Update metrics for all open lots."""
    if today is None:
        today = date.today()

    # Get current prices from holdings
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT stk_cd, crd_class, cur_prc
            FROM holdings
            WHERE snapshot_date = %s
            """,
            (today,),
        )
        price_data = cur.fetchall()

    prices: Dict[Tuple[str, str], Decimal] = {}
    for row in price_data:
        key = (row["stk_cd"], row["crd_class"])
        prices[key] = Decimal(str(row["cur_prc"])) if row["cur_prc"] else Decimal(0)

    # Get all open lots
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT lot_id, stock_code, crd_class, trade_date, avg_purchase_price, net_quantity
            FROM daily_lots
            WHERE is_closed = FALSE
            """
        )
        lots = cur.fetchall()

    updated_count = 0

    with conn.cursor() as cur:
        for lot in lots:
            lot_id = lot["lot_id"]
            stock_code = lot["stock_code"]
            crd_class = lot["crd_class"]
            trade_date = lot["trade_date"]
            avg_price = Decimal(str(lot["avg_purchase_price"]))
            net_qty = lot["net_quantity"]

            current_price = prices.get((stock_code, crd_class))
            holding_days = (today - trade_date).days

            if current_price is not None and current_price > 0:
                unrealized_pnl = (current_price - avg_price) * Decimal(net_qty)
                unrealized_return_pct = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else Decimal(0)
            else:
                current_price = None
                unrealized_pnl = None
                unrealized_return_pct = None

            cur.execute(
                """
                UPDATE daily_lots
                SET holding_days = %s,
                    current_price = %s,
                    unrealized_pnl = %s,
                    unrealized_return_pct = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE lot_id = %s
                """,
                (
                    holding_days,
                    float(current_price) if current_price is not None else None,
                    float(unrealized_pnl) if unrealized_pnl is not None else None,
                    float(unrealized_return_pct) if unrealized_return_pct is not None else None,
                    lot_id,
                ),
            )
            updated_count += 1

    conn.commit()
    return updated_count


def get_open_lots(
    conn: pymysql.connections.Connection,
    stock_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get all open lots, optionally filtered by stock."""
    where_clause = "WHERE is_closed = FALSE"
    params = []

    if stock_code:
        where_clause += " AND stock_code = %s"
        params.append(stock_code)

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                lot_id, stock_code, stock_name, crd_class, trade_date,
                net_quantity, avg_purchase_price, total_cost,
                holding_days, current_price, unrealized_pnl, unrealized_return_pct,
                currency, exchange_code
            FROM daily_lots
            {where_clause}
            ORDER BY unrealized_return_pct DESC
            """,
            params,
        )
        return cur.fetchall()
