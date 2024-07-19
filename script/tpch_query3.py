#!/usr/bin/env python
# coding: utf-8

# # Generating DB
#
# To make our lives easier, we'll use [DuckDBs TPCH extension](https://duckdb.org/docs/extensions/tpch.html) to generate everything in chunks. Let's start with a ~100GB database.

import datetime
import duckdb
import pandas as pd
import time

from tqdm.auto import tqdm


# In[2]:


con = duckdb.connect(database="tpch_sf100.db")

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


def daterange(start_date: datetime.date, end_date: datetime.date, day_jumps=1):
    total_days = int((end_date - start_date).days)
    return (start_date + datetime.timedelta(n) for n in range(0, total_days, day_jumps))


min_orderdate = con.sql("SELECT MIN(o_orderdate) FROM orders").fetchone()[0]
max_orderdate = con.sql("SELECT MAX(o_orderdate) FROM orders").fetchone()[0]

min_shipdate = con.sql("SELECT MIN(l_shipdate) FROM lineitem").fetchone()[0]
max_shipdate = con.sql("SELECT MAX(l_shipdate) FROM lineitem").fetchone()[0]

table = []
for orderdate in tqdm(list(daterange(min_orderdate, max_orderdate, 30)), ncols=80):
    for shipdate in tqdm(list(daterange(min_shipdate, max_shipdate, 30)), leave=False, ncols=80):
        params = {"orderdate": orderdate, "shipdate": shipdate}
        elapsed_times = []
        for trial in range(3):
            start = time.time()
            res = con.sql(query_template, params=params)
            elapsed = time.time() - start
            elapsed_times.append(elapsed)
        elapsed_times = {
            f"elapsed_{trial}": elapsed_times[trial]
            for trial in range(len(elapsed_times))
        }
        params.update(elapsed_times)
        table.append(params)

table = pd.DataFrame(table)
table.to_csv("tpch_q3_sweep.csv")
