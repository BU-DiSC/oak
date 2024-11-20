#!/usr/bin/env python
import duckdb
import time
import pandas as pd
import numpy as np

from tqdm.auto import tqdm

con = duckdb.connect(database="tpch_sf100.db")
con.execute("SET enable_progress_bar = false")

# 6D version
# query_template = """select
#   sum(l_extendedprice* (1 - l_discount)) as revenue
# from
#   lineitem,
#   part
# where
#   (
#       p_partkey = l_partkey
#       and p_brand = 'Brand#24'
#       and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
#       and l_quantity >= $quantity1a and l_quantity <= $quantity1b
#       and p_size between 1 and 5
#       and l_shipmode in ('AIR', 'AIR REG')
#       and l_shipinstruct = 'DELIVER IN PERSON'
#   )
#   or
#   (
#       p_partkey = l_partkey
#       and p_brand = 'Brand#32'
#       and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
#       and l_quantity >= $quantity2a and l_quantity <= $quantity2b
#       and p_size between 1 and 10
#       and l_shipmode in ('AIR', 'AIR REG')
#       and l_shipinstruct = 'DELIVER IN PERSON'
#   )
#   or
#   (
#       p_partkey = l_partkey
#       and p_brand = 'Brand#31'
#       and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
#       and l_quantity >= $quantity3a and l_quantity <= $quantity3b
#       and p_size between 1 and 15
#       and l_shipmode in ('AIR', 'AIR REG')
#       and l_shipinstruct = 'DELIVER IN PERSON'
#   );"""

# 3D version
query_template = """
SELECT
    sum(l_extendedprice* (1 - l_discount)) as revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#24'
        AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= $quantity1
        AND p_size between 1 and 5
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#32'
        AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= $quantity2
        AND p_size between 1 and 10
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#31'
        AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= $quantity3
        AND p_size between 1 and 15
        AND l_shipmode in ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    );
"""


min_quantity = float(con.sql("SELECT MIN(l_quantity) FROM lineitem").fetchone()[0])
max_quantity = float(con.sql("SELECT MAX(l_quantity) FROM lineitem").fetchone()[0])

table = []
NUM_TRIALS = 3

for quantity1 in tqdm(
    list(np.arange(min_quantity, max_quantity + 1.00, 1.00)), ncols=80
):
    for quantity2 in tqdm(
        list(np.arange(min_quantity, max_quantity + 1.00, 1.00)), leave=False, ncols=80
    ):
        for quantity3 in tqdm(
            list(np.arange(min_quantity, max_quantity + 1.00, 1.00)),
            leave=False,
            ncols=80,
        ):
            params = {
                "quantity1": quantity1,
                "quantity2": quantity2,
                "quantity3": quantity3,
            }
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
table.to_csv("tpch_q19_sweep.csv")

con.close()
