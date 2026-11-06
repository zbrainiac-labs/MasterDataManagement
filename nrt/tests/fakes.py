"""Offline fakes for psycopg connections used by the unit tests."""


class FakeCursor:
    """Records SQL calls; answers fetches from a scripted queue."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))

    def executemany(self, sql, rows):
        self._conn.executed_many.append((sql, list(rows)))

    def fetchall(self):
        return self._conn.next_result()

    def fetchone(self):
        return self._conn.next_result()


class FakeConn:
    """Minimal psycopg connection stand-in.

    ``results`` is consumed in order by fetchone/fetchall calls.
    """

    def __init__(self, results=None):
        self.results = list(results or [])
        self.executed = []
        self.executed_many = []
        self.commits = 0
        self.closed = False

    def next_result(self):
        return self.results.pop(0) if self.results else None

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True

    def rows_inserted_into(self, table: str) -> list:
        return [row for sql, rows in self.executed_many if f"INSERT INTO {table}" in sql for row in rows]
