#!/usr/bin/env python
from oak.perturbate import Perturbator
import polars as pl

from oak.db import DuckDBWrapper
from oak.types import Predicate, Query
from tqdm.auto import tqdm

db = DuckDBWrapper(db_path="data/tpch_sf100.db")

query_template_str = """
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
            THEN 1
        ELSE 0
    END) as high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
            THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('AIR', 'REG AIR')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= $receiptdate1
    AND l_receiptdate < $receiptdate2
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
"""

min_receiptdate = db.con.sql("SELECT MIN(l_receiptdate) FROM lineitem").fetchone()
assert min_receiptdate is not None
min_receiptdate = min_receiptdate[0]

max_receiptdate = db.con.sql("SELECT MAX(l_receiptdate) FROM lineitem").fetchone()
assert max_receiptdate is not None
max_receiptdate = max_receiptdate[0]

query = Query(
    query_str=query_template_str,
    predicates=[
        Predicate(
            name="receiptdate1",
            value=0,
            minimum=min_receiptdate,
            maximum=max_receiptdate,
        ),
        Predicate(
            name="receiptdate2",
            value=0,
            minimum=min_receiptdate,
            maximum=max_receiptdate,
        ),
    ],
)

perturbator = Perturbator(query, 1)

NUM_TRIALS = 3
NUM_SAMPLES = 100
result_table = []
for _ in tqdm(range(NUM_SAMPLES), ncols=80):
    row = {}
    perturbed_predicates = perturbator.gen_perturbed_predicate()
    for trial in range(NUM_TRIALS):
        res_time, _ = db.execute_query(
            Query(query_str=query.query_str, predicates=perturbed_predicates)
        )
        row[f"elapsed_{trial}"] = res_time
    result_table.append(row)

table = pl.DataFrame(result_table)
table.to_csv("tpch_q12_sweep.csv")

db.close()
