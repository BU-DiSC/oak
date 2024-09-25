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

# # TPCH Query 3
#
# ```sql
# SELECT
#     l_orderkey,
#     sum(l_extendedprice * (1 - l_discount)) as revenue,
#     o_orderdate,
#     o_shippriority
# FROM
#     customer,
#     orders,
#     lineitem
# WHERE
#     c_mktsegment = 'BUILDING'
#     AND c_custkey = o_custkey
#     AND l_orderkey = o_orderkey
#     AND o_orderdate < date '1995-03-15'
#     AND l_shipdate > date '1995-03-15'
# GROUP BY
#     l_orderkey,
#     o_orderdate,
#     o_shippriority
# ORDER BY
#     revenue desc,
#     o_orderdate
# LIMIT 20;
# ```
#
# In this instance we will be changing `o_orderdate` and `l_shipdate` predicates


query_template = """
    SELECT
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        lineitem
    WHERE
        c_mktsegment = 'BUILDING'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < $orderdate
        AND l_shipdate > $shipdate
    GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
    ORDER BY
        revenue desc,
        o_orderdate
    LIMIT 20;
"""


#min_orderdate = con.sql("SELECT MIN(o_orderdate) FROM orders").fetchone()[0]
#max_orderdate = con.sql("SELECT MAX(o_orderdate) FROM orders").fetchone()[0]

#min_shipdate = con.sql("SELECT MIN(l_shipdate) FROM lineitem").fetchone()[0]
#max_shipdate = con.sql("SELECT MAX(l_shipdate) FROM lineitem").fetchone()[0]

table = pd.read_csv("tpch_q3_sweep.csv",index_col=0)

min_orderdate = min(table['orderdate'])
min_shipdate = min(table['shipdate'])
max_orderdate = max(table['orderdate'])
max_shipdate = max(table['shipdate'])

NUM_TRIALS = 3
orderdate_skipby = '30D'
shipdate_skipby = '30D'

value_orders={}
value_orders['orderdate']=list(pd.date_range(min_orderdate, max_orderdate, freq=orderdate_skipby).strftime("%Y-%m-%d"))
value_orders['shipdate']=list(pd.date_range(min_shipdate, max_shipdate, freq=shipdate_skipby).strftime("%Y-%m-%d"))

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


qt_orderdate = """
    SELECT count(*)
    FROM
        orders
    WHERE
        o_orderdate < $orderdate
        ;
"""
qt_shipdate = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_shipdate > $shipdate
   ;
"""
sel_orderdate={}
sel_shipdate={}
for orderdate in tqdm(value_orders['orderdate'], ncols=80):
    sel_orderdate[orderdate] = con.sql(qt_orderdate, params={"orderdate": orderdate}).fetchone()[0]

for shipdate in tqdm(value_orders['shipdate'], leave=False, ncols=80):
    sel_shipdate[shipdate] = con.sql(qt_shipdate, params={"shipdate": shipdate}).fetchone()[0]
    
orders_card = con.sql("SELECT count(*) FROM orders").fetchone()[0]
lineitem_card = con.sql("SELECT count(*) FROM lineitem").fetchone()[0]

print('sel_shipdate:', {x: y/lineitem_card for x,y in sel_shipdate.items()})
print('sel_orderdate', {x: y/orders_card for x,y in sel_orderdate.items()})