import os
import sqlite3
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional

# =====================================================================
# CONFIGURATION
# =====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "nex_ledger.db")

LOCAL_MODE = True
TELEMETRY_ENABLED = False
VERIFICATION_INTERVAL = 1.0  # Seconds between loop executions
MAX_EVENTS = 100_000  # Prevent unbounded DB growth
MAX_EVENT_DATA_LENGTH = 4096  # Sanity cap on event payload size


# =====================================================================
# CORE LEDGER ENGINE
# =====================================================================
class NexLedger:
    """Local SQLite hash-chained event ledger.

    Each row stores a timestamp, event payload, the previous row's hash,
    and the current row's SHA-256 hash.  The chain is verified by
    re-computing every hash from genesis forward.
    """

    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Return a new SQLite connection (short-lived, thread-safe)."""
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        """Initialize local SQLite ledger table if it does not exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS state_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    current_hash TEXT NOT NULL
                )
                '''
            )
            conn.commit()

    def get_last_hash(self) -> str:
        """Retrieve the current chain tip hash (genesis hash if empty)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT current_hash FROM state_ledger ORDER BY id DESC LIMIT 1'
            )
            row = cursor.fetchone()
            return row[0] if row else "0" * 64

    def _prune_old_events(self, conn: sqlite3.Connection) -> None:
        """Delete oldest events beyond MAX_EVENTS to bound DB size."""
        cursor = conn.cursor()
        cursor.execute(
            'DELETE FROM state_ledger WHERE id NOT IN '
            '(SELECT id FROM state_ledger ORDER BY id DESC LIMIT ?)',
            (MAX_EVENTS,),
        )

    def commit_event(self, event_data: str) -> str:
        """Append a local event to the hash chain and return its hash.

        Raises ValueError if event_data is empty or exceeds MAX_EVENT_DATA_LENGTH.
        """
        if not event_data:
            raise ValueError("event_data must not be empty")
        if len(event_data) > MAX_EVENT_DATA_LENGTH:
            raise ValueError(
                f"event_data exceeds max length of {MAX_EVENT_DATA_LENGTH}"
            )

        prev_hash = self.get_last_hash()
        timestamp = datetime.now(timezone.utc).isoformat()

        raw_payload = f"{timestamp}:{event_data}:{prev_hash}"
        curr_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO state_ledger (timestamp, event_data, previous_hash, current_hash)
                VALUES (?, ?, ?, ?)
                ''',
                (timestamp, event_data, prev_hash, curr_hash),
            )
            self._prune_old_events(conn)
            conn.commit()

        return curr_hash

    def verify_ledger_integrity(self) -> bool:
        """Verify the integrity of the local cryptographic chain.

        Re-computes every hash from genesis forward and checks that
        each row's previous_hash matches the prior row's current_hash.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT timestamp, event_data, previous_hash, current_hash '
                'FROM state_ledger ORDER BY id ASC'
            )
            rows = cursor.fetchall()

            expected_prev_hash = "0" * 64
            for timestamp, event_data, prev_hash, curr_hash in rows:
                if prev_hash != expected_prev_hash:
                    return False

                raw_payload = f"{timestamp}:{event_data}:{prev_hash}"
                calculated_hash = hashlib.sha256(
                    raw_payload.encode("utf-8")
                ).hexdigest()
                if calculated_hash != curr_hash:
                    return False

                expected_prev_hash = curr_hash

        return True

    def get_event_count(self) -> int:
        """Return the total number of events in the ledger."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM state_ledger')
            row = cursor.fetchone()
            return row[0] if row else 0


# =====================================================================
# LOCAL EXECUTION LOOP
# =====================================================================
def run_nex_node() -> None:
    """Run the local NEX node loop, committing events at a fixed interval."""
    print("=" * 60)
    print("NEX LOCAL NODE INITIALIZED")
    print(f"Database Path : {DB_PATH}")
    print(
        f"Telemetry     : "
        f"{'ENABLED' if TELEMETRY_ENABLED else 'DISABLED (LOCAL SOVEREIGN)'}"
    )
    print("=" * 60)

    ledger = NexLedger()

    # Initial state bootstrap
    init_hash = ledger.commit_event("SYSTEM_BOOTSTRAP_LOCAL_EXECUTION")
    print(f"[BOOTSTRAP] Genesis state committed. Hash: {init_hash[:16]}...")

    try:
        while True:
            event_count = ledger.get_event_count()
            event_data = f"HEARTBEAT_EVENT_{event_count}"
            curr_hash = ledger.commit_event(event_data)
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"Event #{event_count} committed. Hash: {curr_hash[:16]}..."
            )

            # Verify integrity every 60 events
            if event_count % 60 == 0:
                integrity_ok = ledger.verify_ledger_integrity()
                status = "VALID" if integrity_ok else "COMPROMISED"
                print(f"[INTEGRITY CHECK] Chain status: {status}")

            time.sleep(VERIFICATION_INTERVAL)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] NEX node stopped by user.")


if __name__ == "__main__":
    run_nex_node()
