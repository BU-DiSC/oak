#!/usr/bin/env python
import duckdb
import time
import pandas as pd
from tqdm.auto import tqdm
import numpy as np

con = duckdb.connect(database="tpch_sf100.db")
con.execute("SET enable_progress_bar = false")

# 5D version
# query_template = """SELECT
# 	sum(l_extendedprice * l_discount) AS revenue
# FROM
# 	lineitem
# WHERE
# 	l_shipdate >= '$shipdate1'
# 	AND l_shipdate < '$shipdate2'
# 	AND l_discount >= '$discount1' 
#     AND l_discount < '$discount2'
# 	AND l_quantity < '$quantity';
# """

# 3D version
query_template = """
SELECT
	sum(l_extendedprice * l_discount) AS revenue
	FROM
	lineitem
	WHERE
	l_shipdate >= $shipdate
	AND l_discount >= $discount
	AND l_quantity < $quantity;
"""

min_shipdate = con.sql("SELECT MIN(l_shipdate) FROM lineitem").fetchone()[0]
max_shipdate = con.sql("SELECT MAX(l_shipdate) FROM lineitem").fetchone()[0]

min_discount = float(con.sql("SELECT MIN(l_discount) FROM lineitem").fetchone()[0])
max_discount = float(con.sql("SELECT MAX(l_discount) FROM lineitem").fetchone()[0])

min_quantity = float(con.sql("SELECT MIN(l_quantity) FROM lineitem").fetchone()[0])
max_quantity = float(con.sql("SELECT MAX(l_quantity) FROM lineitem").fetchone()[0])

table = []
NUM_TRIALS = 3

for shipdate in tqdm(list(pd.date_range(min_shipdate, max_shipdate, freq='30D'))):
    for discount in tqdm(list(np.arange(min_discount, max_discount + 0.01, 0.01)), leave=False):
        for quantity in tqdm(list(np.arange(min_quantity, max_quantity + 1.00, 1.00)), leave=False):
            params = {'shipdate': shipdate, 'discount': discount, 'quantity': quantity}
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
table.to_csv('tpch_q6_sweep.csv')

con.close()
