#!/usr/bin/env python
import duckdb
import time
import pandas as pd
from tqdm.auto import tqdm

con = duckdb.connect(database="tpch_sf100.db")
con.execute("SET enable_progress_bar = false")

query_template = """
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

min_receiptdate = con.sql("SELECT MIN(l_receiptdate) FROM lineitem").fetchone()[0]
max_receiptdate = con.sql("SELECT MAX(l_receiptdate) FROM lineitem").fetchone()[0]

table = []
NUM_TRIALS = 3

for receiptdate1 in tqdm(
    list(pd.date_range(min_receiptdate, max_receiptdate, freq="30D")),
    ncols=90,
):
    for receiptdate2 in tqdm(
        list(pd.date_range(min_receiptdate, max_receiptdate, freq="30D")),
        ncols=80,
        leave=False,
    ):
        params = {"receiptdate1": receiptdate1, "receiptdate2": receiptdate2}
        row = dict()
        for trial in range(NUM_TRIALS):
            start = time.time()
            res = con.sql(query_template, params=params)
            elapsed = time.time() - start
            row[f"elapsed_{trial}"] = elapsed
            # If we want to, save the result for a sanity check
            # row[f'res_{trial}'] = res.fetchall()
        table.append({**params, **row})

table = pd.DataFrame(table)
table.to_csv("tpch_q12_sweep.csv")

con.close()
