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


table = pd.read_csv("tpch_q12_sweep.csv",index_col=0)

min_receiptdate1 = min(table['receiptdate1'])
max_receiptdate1 = max(table['receiptdate1'])
min_receiptdate2 = min(table['receiptdate2'])
max_receiptdate2 = max(table['receiptdate2'])

NUM_TRIALS = 3
receiptdate1_skipby = '30D'
receiptdate2_skipby = '30D'

value_orders={}
value_orders['receiptdate1']=list(pd.date_range(min_receiptdate1, max_receiptdate1, freq=receiptdate1_skipby).strftime("%Y-%m-%d"))
value_orders['receiptdate2']=list(pd.date_range(min_receiptdate2, max_receiptdate2, freq=receiptdate2_skipby).strftime("%Y-%m-%d"))

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


qt_receiptdate1 = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_receiptdate >= $receiptdate1
        ;
"""
qt_receiptdate2 = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_receiptdate < $receiptdate2
   ;
"""
sel_receiptdate1={}
sel_receiptdate2={}
for receiptdate1 in tqdm(value_orders['receiptdate1'], ncols=80):
    sel_receiptdate1[receiptdate1] = con.sql(qt_receiptdate1, params={"receiptdate1": receiptdate1}).fetchone()[0]

for receiptdate2 in tqdm(value_orders['receiptdate2'], ncols=80):
    sel_receiptdate2[receiptdate2] = con.sql(qt_receiptdate2, params={"receiptdate2": receiptdate2}).fetchone()[0]
    
lineitem_card = con.sql("SELECT count(*) FROM lineitem").fetchone()[0]

print('sel_receiptdate1:', {x: y/lineitem_card for x,y in sel_receiptdate1.items()})
print('sel_receiptdate2', {x: y/lineitem_card for x,y in sel_receiptdate2.items()})