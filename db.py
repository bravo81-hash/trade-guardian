import sqlite3
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional

DB_PATH = "allantis.db"


# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------
def get_connection():
    """Returns a SQLite connection with row access by name."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------
# SCHEMA CREATION
# ---------------------------------------------------------
def init_db():
    """Create tables if missing (idempotent)."""

    conn = get_connection()
    cur = conn.cursor()

    # TRADES TABLE, one row per trade
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            name TEXT,
            strategy TEXT,
            status TEXT,
            pnl REAL,
            debit REAL,
            debit_per_lot REAL,
            grade TEXT,
            reason TEXT,
            alerts TEXT,
            days_held INTEGER,
            daily_yield REAL,
            roi REAL,
            entry_date TEXT,
            expiration_date TEXT,
            lot_size INTEGER,
            latest_flag INTEGER DEFAULT 1
        );
    """)

    # GREEKS TABLE
    cur.execute("""
        CREATE TABLE IF NOT EXISTS greeks (
            trade_id TEXT,
            theta REAL,
            delta REAL,
            gamma REAL,
            vega REAL,
            FOREIGN KEY(trade_id) REFERENCES trades(trade_id)
        );
    """)

    # LEGS TABLE (OPTIONSTRAT legs)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS legs (
            trade_id TEXT,
            symbol TEXT,
            quantity REAL,
            entry_price REAL,
            current_price REAL,
            FOREIGN KEY(trade_id) REFERENCES trades(trade_id)
        );
    """)

    # MULTI-NOTE TABLE
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            timestamp TEXT,
            content TEXT,
            FOREIGN KEY(trade_id) REFERENCES trades(trade_id)
        );
    """)

    conn.commit()
    conn.close()


# Initialize DB on import
init_db()


# ---------------------------------------------------------
# UTILITY FUNCTIONS
# ---------------------------------------------------------
def make_trade_id(name: str, created_at: str, strategy: str) -> str:
    """
    Creates a stable unique ID per trade.
    Based on Name + Created At + Strategy.
    """
    base = f"{name}|{created_at}|{strategy}".encode()
    return hashlib.sha256(base).hexdigest()[:16]


# ---------------------------------------------------------
# INSERT / UPDATE HELPERS
# ---------------------------------------------------------
def upsert_trade(trade: Dict[str, Any]):
    """
    Insert or update a trade. Idempotent.
    trade = {
        trade_id, name, strategy, status, pnl, debit, debit_per_lot,
        grade, reason, alerts, days_held, daily_yield, roi,
        entry_date, expiration_date, lot_size, latest_flag
    }
    """

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO trades (
            trade_id, name, strategy, status,
            pnl, debit, debit_per_lot, grade, reason, alerts,
            days_held, daily_yield, roi, entry_date, expiration_date,
            lot_size, latest_flag
        )
        VALUES (
            :trade_id, :name, :strategy, :status,
            :pnl, :debit, :debit_per_lot, :grade, :reason, :alerts,
            :days_held, :daily_yield, :roi, :entry_date, :expiration_date,
            :lot_size, :latest_flag
        )
        ON CONFLICT(trade_id) DO UPDATE SET
            status=excluded.status,
            pnl=excluded.pnl,
            debit=excluded.debit,
            debit_per_lot=excluded.debit_per_lot,
            grade=excluded.grade,
            reason=excluded.reason,
            alerts=excluded.alerts,
            days_held=excluded.days_held,
            daily_yield=excluded.daily_yield,
            roi=excluded.roi,
            expiration_date=excluded.expiration_date,
            latest_flag=excluded.latest_flag;
    """, trade)

    conn.commit()
    conn.close()


def replace_greeks(trade_id: str, theta: float, delta: float, gamma: float, vega: float):
    """Store greeks for a trade (delete then insert)."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM greeks WHERE trade_id=?", (trade_id,))
    cur.execute("""
        INSERT INTO greeks (trade_id, theta, delta, gamma, vega)
        VALUES (?, ?, ?, ?, ?)
    """, (trade_id, theta, delta, gamma, vega))

    conn.commit()
    conn.close()


def replace_legs(trade_id: str, legs: List[Dict[str, Any]]):
    """Store all legs for a trade (delete then insert)."""

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM legs WHERE trade_id=?", (trade_id,))

    for leg in legs:
        cur.execute("""
            INSERT INTO legs (trade_id, symbol, quantity, entry_price, current_price)
            VALUES (?, ?, ?, ?, ?)
        """, (
            trade_id,
            leg.get("symbol"),
            leg.get("quantity"),
            leg.get("entry_price"),
            leg.get("current_price")
        ))

    conn.commit()
    conn.close()


def add_note(trade_id: str, content: str):
    """Add a new note to a trade."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO notes (trade_id, timestamp, content)
        VALUES (?, ?, ?)
    """, (trade_id, datetime.now().isoformat(), content))

    conn.commit()
    conn.close()


# ---------------------------------------------------------
# QUERY HELPERS
# ---------------------------------------------------------
def get_trade_notes(trade_id: str) -> List[sqlite3.Row]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM notes
        WHERE trade_id=?
        ORDER BY timestamp ASC
    """, (trade_id,))

    rows = cur.fetchall()
    conn.close()
    return rows


def load_all_trades() -> List[sqlite3.Row]:
    """Return all trades in DB for analytics."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM trades ORDER BY entry_date DESC;")
    rows = cur.fetchall()

    conn.close()
    return rows


def load_active_trades() -> List[sqlite3.Row]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM trades
        WHERE status='Active'
        AND latest_flag=1
        ORDER BY entry_date DESC;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


def fetch_greeks(trade_id: str) -> Optional[sqlite3.Row]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM greeks WHERE trade_id=?", (trade_id,))
    row = cur.fetchone()

    conn.close()
    return row


def fetch_legs(trade_id: str) -> List[sqlite3.Row]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM legs WHERE trade_id=?", (trade_id,))
    rows = cur.fetchall()

    conn.close()
    return rows
