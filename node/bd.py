import os
import sqlite3
import threading

class Storage:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()

        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_term INTEGER NOT NULL,
                    voted_for INTEGER,
                    last_applied INTEGER NOT NULL
                )
            """)
            self.conn.execute(
                "INSERT OR IGNORE INTO meta VALUES (1, 0, NULL, 0)"
            )
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    idx INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    chave TEXT,
                    valor TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS data (
                    chave TEXT PRIMARY KEY,
                    valor TEXT
                )
            """)

    def carrega(self):
        with self._lock:
            term, voted_for, last_applied = self.conn.execute(
                "SELECT current_term, voted_for, last_applied FROM meta"
            ).fetchone()

            log = [
                {
                    "index": idx,
                    "term": t,
                    "operation": op,
                    "key": k,
                    "value": v
                }
                for idx, t, op, k, v in self.conn.execute(
                    "SELECT idx, term, operation, chave, valor "
                    "FROM log ORDER BY idx"
                )
            ]

            db = dict(self.conn.execute("SELECT chave, valor FROM data"))

        return term, voted_for, last_applied, log, db

    def save_meta(self, term, voted_for):
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE meta SET current_term = ?, voted_for = ? WHERE id = 1",
                (term, voted_for)
            )

    def adiciona_log(self, entries):
        with self._lock, self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO log VALUES (?, ?, ?, ?, ?)",
                [
                    (e["index"], e["term"], e["operation"],
                     e["key"], e["value"])
                    for e in entries
                ]
            )

    def trunca_log(self, to_index):
        with self._lock, self.conn:
            self.conn.execute("DELETE FROM log WHERE idx > ?", (to_index,))

    def apply(self, puts, last_applied):
    
        with self._lock, self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO data VALUES (?, ?)",
                puts
            )
            self.conn.execute(
                "UPDATE meta SET last_applied = ? WHERE id = 1",
                (last_applied,)
            )

    def close(self):
        with self._lock:
            self.conn.close()