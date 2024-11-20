#!/usr/bin/env python
import duckdb
import time
import pandas as pd
from tqdm.auto import tqdm

con = duckdb.connect(database="tpch_sf1.db")
con.execute("SET enable_progress_bar = false")

#2D
query_template = """
SELECT s_name, s_address
FROM supplier,nation
WHERE
	s_suppkey in (
		SELECT ps_suppkey
		FROM partsupp
		WHERE
			ps_partkey in (
				SELECT p_partkey
				FROM part
				WHERE p_name like 'navajo%'
			)
			AND ps_availqty > (
				SELECT 0.5 * sum(l_quantity)
				FROM lineitem
				WHERE
					l_partkey = ps_partkey
					AND l_suppkey = ps_suppkey
					AND l_shipdate >= $shipdate1
					AND l_shipdate < $shipdate2
			)
	)
	AND s_nationkey = n_nationkey AND n_name = 'ETHIOPIA'
ORDER BY s_name;
"""

min_shipdate = con.sql("SELECT MIN(l_shipdate) FROM lineitem").fetchone()[0]
max_shipdate = con.sql("SELECT MAX(l_shipdate) FROM lineitem").fetchone()[0]

table = []
NUM_TRIALS = 3

for shipdate1 in tqdm(list(pd.date_range(min_shipdate, max_shipdate, freq='30D'))):
    for shipdate2 in tqdm(list(pd.date_range(min_shipdate, max_shipdate, freq='30D')), leave=False):
        params = {'shipdate1': shipdate1, 'shipdate2': shipdate2}
        row = dict()
        for trial in range(NUM_TRIALS):
            start = time.time()
            res = con.sql(query_template, params=params)
            elapsed = time.time() - start
            row[f'elapsed_{trial}'] = elapsed
            # If we want to, save the result for a sanity check
            # row[f'res_{trial}'] = res.fetchall()
        table.append({**params, **row})

table = pd.DataFrame(table)
table.to_csv('tpch_q20_sweep.csv')

con.close()

