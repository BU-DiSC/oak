import sqlite3
from time import time
from typing import Tuple

from oak.types import Query


class SqliteWrapper:
    def __init__(self, db_path: str) -> None:
        con = sqlite3.connect(db_path)

        self.con : sqlite3.Connection = con

    def execute_query(self, query: Query) -> Tuple[float, sqlite3.Cursor]:
        params = list(pred.value for pred in query.predicates)
        start = time()
        res = self.con.execute(query.query_str, params)
        elapsed = time() - start

        return elapsed, res

    def close(self):
        self.con.close()
