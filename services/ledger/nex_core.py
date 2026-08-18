import os
import sqlite3
import hashlib
import time
from datetime import datetime

# =====================================================================
# CONFIGURATION
# =====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "nex_ledger.db")

LOCAL_MODE = True
TELEMETRY_ENABLED = False
VERIFICATION_INTERVAL = 1.0  # Seconds between loop executions

MAX_EVENT_DATA_LENGTH = 4096  # Prevent unbounded event_data payloads


# =====================================================================
# CORE LEDGER ENGINE
# =====================================================================
class NexLedger:
    """Local SQLite-backed hash-chained event ledger.

    Each event is appended as a row linking to the previous row's hash,
    forming a tamper-evident chain.  Integrity can be verified at any time
    via :meth:`verify_ledger_integrity`.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
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
        """Retrieve the current chain tip hash."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT current_hash FROM state_ledger ORDER BY id DESC LIMIT 1'
            )
            row = cursor.fetchone()
            return row[0] if row else "0" * 64

    def commit_event(self, event_data: str) -> str:
        """Append a local event state to the hash chain.

        Args:
            event_data: Arbitrary string payload describing the event.
                        Truncated to MAX_EVENT_DATA_LENGTH characters.

        Returns:
            The SHA-256 hash of the newly committed event.
        """
        if not isinstance(event_data, str):
            raise TypeError("event_data must be a string")
        if len(event_data) > MAX_EVENT_DATA_LENGTH:
            event_data = event_data[:MAX_EVENT_DATA_LENGTH]

        prev_hash = self.get_last_hash()
        timestamp = datetime.utcnow().isoformat()

        raw_payload = f"{timestamp}:{event_data}:{prev_hash}"
        curr_hash = hashlib.sha256(raw_payload.encode('utf-8')).hexdigest()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO state_ledger (timestamp, event_data, previous_hash, current_hash)
                VALUES (?, ?, ?, ?)
                ''',
                (timestamp, event_data, prev_hash, curr_hash),
            )
            conn.commit()

        return curr_hash

    def verify_ledger_integrity(self) -> bool:
        """Verify the integrity of the local cryptographic chain.

        Walks every row in order, recomputes each hash, and checks that
        the ``previous_hash`` field matches the prior row's ``current_hash``.

        Returns:
            True if the chain is intact, False if any entry is tampered.
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
                    raw_payload.encode('utf-8')
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
    """Run the NEX local node loop.

    Bootstraps the ledger, commits a genesis event, then enters an
    infinite loop committing a heartbeat event every VERIFICATION_INTERVAL
    seconds and verifying chain integrity on each iteration.
    """
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

    # Main loop
    iteration = 0
    try:
        while True:
            iteration += 1
            time.sleep(VERIFICATION_INTERVAL)

            event_hash = ledger.commit_event(f"HEARTBEAT_ITERATION_{iteration}")
            integrity_ok = ledger.verify_ledger_integrity()

            print(
                f"[LOOP] iter={iteration} "
                f"hash={event_hash[:16]}... "
                f"integrity={'OK' if integrity_ok else 'FAILED'} "
                f"events={ledger.get_event_count()}"
            )

            if not integrity_ok:
                print("[ALERT] Ledger integrity check FAILED — halting node.")
                break
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] NEX node stopped by user.")


if __name__ == "__main__":
    run_nex_node()
