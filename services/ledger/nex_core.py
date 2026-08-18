import os
import sqlite3
import hashlib
from datetime import datetime, timezone
import time

# =====================================================================
# CONFIGURATION
# =====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "nex_ledger.db")

LOCAL_MODE = True
TELEMETRY_ENABLED = False
VERIFICATION_INTERVAL = 1.0  # Seconds between loop executions


# =====================================================================
# CORE LEDGER ENGINE
# =====================================================================
class NexLedger:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Initialize local SQLite ledger table if it does not exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS state_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    current_hash TEXT NOT NULL
                )
            ''')
            conn.commit()

    def get_last_hash(self) -> str:
        """Retrieve the current chain tip hash."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT current_hash FROM state_ledger ORDER BY id DESC LIMIT 1')
            row = cursor.fetchone()
            return row[0] if row else "0" * 64

    def commit_event(self, event_data: str) -> str:
        """Append an unconstrained local event state to the hash chain."""
        prev_hash = self.get_last_hash()
        timestamp = datetime.now(timezone.utc).isoformat()

        # Payload verification string
        raw_payload = f"{timestamp}:{event_data}:{prev_hash}"
        curr_hash = hashlib.sha256(raw_payload.encode('utf-8')).hexdigest()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO state_ledger (timestamp, event_data, previous_hash, current_hash)
                VALUES (?, ?, ?, ?)
            ''', (timestamp, event_data, prev_hash, curr_hash))
            conn.commit()

        return curr_hash

    def verify_ledger_integrity(self) -> bool:
        """Verify the integrity of the local cryptographic chain."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT timestamp, event_data, previous_hash, current_hash FROM state_ledger ORDER BY id ASC')
            rows = cursor.fetchall()

            expected_prev_hash = "0" * 64
            for timestamp, event_data, prev_hash, curr_hash in rows:
                if prev_hash != expected_prev_hash:
                    return False

                raw_payload = f"{timestamp}:{event_data}:{prev_hash}"
                calculated_hash = hashlib.sha256(raw_payload.encode('utf-8')).hexdigest()
                if calculated_hash != curr_hash:
                    return False

                expected_prev_hash = curr_hash

        return True


# =====================================================================
# LOCAL EXECUTION LOOP
# =====================================================================
def run_nex_node():
    print("=" * 60)
    print("NEX LOCAL NODE INITIALIZED")
    print(f"Database Path : {DB_PATH}")
    print(f"Telemetry     : {'ENABLED' if TELEMETRY_ENABLED else 'DISABLED (LOCAL SOVEREIGN)'}")
    print("=" * 60)

    ledger = NexLedger()

    # Initial state bootstrap
    init_hash = ledger.commit_event("SYSTEM_BOOTSTRAP_LOCAL_EXECUTION")
    print(f"[BOOTSTRAP] Genesis state committed. Hash: {init_hash[:16]}...")

    try:
        while True:
            integrity_ok = ledger.verify_ledger_integrity()
            if not integrity_ok:
                print("[ALERT] Ledger integrity check FAILED — tampering detected.")
                break

            event_hash = ledger.commit_event("LOCAL_HEARTBEAT")
            print(f"[COMMIT] Event committed. Hash: {event_hash[:16]}... | Integrity: OK")

            time.sleep(VERIFICATION_INTERVAL)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] NEX node stopped by user.")


if __name__ == "__main__":
    run_nex_node()
