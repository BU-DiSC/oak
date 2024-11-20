from time import time
from typing import Tuple

import duckdb
from oak.types import Query


class DuckDBWrapper:
    def __init__(self, db_path: str) -> None:
        con = duckdb.connect(database=db_path)
        con.execute("SET enable_progress_bar = false")

        self.con: duckdb.DuckDBPyConnection = con

    def execute_query(self, query: Query) -> Tuple[float, duckdb.DuckDBPyRelation]:
        params = {pred.name: pred.value for pred in query.predicates}
        start = time()
        res = self.con.sql(query.query_str, params=params)
        elapsed = time() - start

        return elapsed, res

    def close(self):
        self.con.close()
