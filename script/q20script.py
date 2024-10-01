#!/usr/bin/env python
# coding: utf-8

# # Generating DB
#
# To make our lives easier, we'll use [DuckDBs TPCH extension](https://duckdb.org/docs/extensions/tpch.html) to generate everything in chunks. Let's start with a ~100GB database.

import datetime
import duckdb
import pandas as pd
import time
import itertools
from tqdm.auto import tqdm


# In[2]:


con = duckdb.connect(database="/root/venv/tpch_sf100.db")

# con.execute("INSTALL tpch; LOAD tpch")
# for idx in tqdm(range(10)):
#     con.execute(f"CALL dbgen(sf=100, children=10, step={idx})")

con.execute("SET enable_progress_bar = false")


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


table = pd.read_csv("tpch_q20_sweep.csv",index_col=0)

min_shipdate1 = min(table['shipdate1'])
max_shipdate1 = max(table['shipdate1'])
min_shipdate2 = min(table['shipdate2'])
max_shipdate2 = max(table['shipdate2'])

NUM_TRIALS = 3
shipdate1_skipby = '30D'
shipdate2_skipby = '30D'

value_orders={}
value_orders['shipdate1']=list(pd.date_range(min_shipdate1, max_shipdate1, freq=shipdate1_skipby).strftime("%Y-%m-%d"))
value_orders['shipdate2']=list(pd.date_range(min_shipdate2, max_shipdate2, freq=shipdate2_skipby).strftime("%Y-%m-%d"))

#print(table)
table_initial = table.iloc[:, :-3]  # All columns except the last three


# Create a lookup dictionary for fast index lookups
value_order_dict = {
    key: {v: i for i, v in enumerate(value_orders[key])} 
    for key in value_orders
}

def are_adjacent(value1, value2, order_dict):
    return abs(order_dict[value1] - order_dict[value2]) <= 1


def are_neighbors(row1, row2):
    return all(are_adjacent(a, b, value_order_dict[col]) for a, b, col in zip(row1, row2, table_initial.columns))

# Find all pairs of neighbor rows
neighbor_pairs = []


# Compare each pair of rows
for i, j in itertools.combinations(range(len(table_initial)), 2):
    if are_neighbors(table_initial.iloc[i], table_initial.iloc[j]):
        neighbor_pairs.append((i, j))


table['mean_elapsed'] = 0
for trial in range(NUM_TRIALS):
    table['mean_elapsed'] += table[f"elapsed_{trial}"]
table['mean_elapsed'] = table['mean_elapsed']/NUM_TRIALS

deviation_log = []
for i, (row1_idx, row2_idx) in enumerate(neighbor_pairs):
    # Extract values from the DataFrame
    value1 = table.loc[row1_idx, 'mean_elapsed']
    value2 = table.loc[row2_idx, 'mean_elapsed']
    deviation = max(value1 / value2, value2 / value1)
    
    # Store the deviation value
    deviation_log.append([row1_idx, row2_idx, deviation])
    
print(deviation_log)


qt_shipdate1 = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_shipdate >= $shipdate1
        ;
"""
qt_shipdate2 = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_shipdate < $shipdate2
   ;
"""
sel_shipdate1={}
sel_shipdate2={}
for shipdate1 in tqdm(value_orders['shipdate1'], ncols=80):
    sel_shipdate1[shipdate1] = con.sql(qt_shipdate1, params={"shipdate1": shipdate1}).fetchone()[0]

for shipdate2 in tqdm(value_orders['shipdate2'], ncols=80):
    sel_shipdate2[shipdate2] = con.sql(qt_shipdate2, params={"shipdate2": shipdate2}).fetchone()[0]
    
lineitem_card = con.sql("SELECT count(*) FROM lineitem").fetchone()[0]

print('sel_shipdate1:', {x: y/lineitem_card for x,y in sel_shipdate1.items()})
print('sel_shipdate2', {x: y/lineitem_card for x,y in sel_shipdate2.items()})