#!/usr/bin/env python
import duckdb
import time
import pandas as pd
from tqdm.auto import tqdm
import numpy as np
import itertools

con = duckdb.connect(database="/root/venv/tpch_sf100.db")
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

discount_skipby = 0.01
shipdate_skipby = '30D'
quantity_skipby = 1.00

value_orders={}
value_orders['discount']=list(np.arange(min_discount, max_discount + 0.01, discount_skipby))
value_orders['shipdate']=list(pd.date_range(min_shipdate, max_shipdate, freq=shipdate_skipby))
value_orders['quantity']=list(np.arange(min_quantity, max_quantity + 1.00, quantity_skipby))


for shipdate in tqdm(value_orders['shipdate'], ncols=80):
    for discount in tqdm(value_orders['discount'], leave=False, ncols=80):
        for quantity in tqdm(value_orders['quantity'], leave=False, ncols=80):
            params = {'shipdate': shipdate, 'discount': discount, 'quantity': quantity}
            elapsed_times = []
            for trial in range(NUM_TRIALS):
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
table.to_csv('tpch_q6_sweep.csv')


table_initial = table.iloc[:, :-3]  # All columns except the last three

def are_adjacent(value1, value2, order):
    return abs(order.index(value1) - order.index(value2)) == 1

def are_neighbors(row1, row2):
    return all(are_adjacent(a, b, value_orders[col]) for a, b, col in zip(row1, row2, table_initial.columns))

# Find all pairs of neighbor rows
neighbor_pairs = []

# Compare each pair of rows
for i, j in itertools.combinations(range(len(table_initial)), 2):
    if are_neighbors(table_initial.iloc[i], table_initial.iloc[j]):
        neighbor_pairs.append((i, j))
        #print(table_initial.iloc[i], table_initial.iloc[j])


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

qt_discount = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
        l_discount >= $discount
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

qt_quantity = """
    SELECT count(*)
    FROM
        lineitem
    WHERE
       l_quantity < $quantity;
        ;
"""

sel_discount={}
sel_shipdate={}
sel_quantity={}
for discount in tqdm(value_orders['discount'], ncols=80):
    sel_discount[discount] = con.sql(qt_discount, params={"discount": discount}).fetchone()[0]

for shipdate in tqdm(value_orders['shipdate'], leave=False, ncols=80):
    sel_shipdate[shipdate] = con.sql(qt_shipdate, params={"shipdate": shipdate}).fetchone()[0]

for quantity in tqdm(value_orders['quantity'], ncols=80):
    sel_quantity[quantity] = con.sql(qt_quantity, params={"quantity": quantity}).fetchone()[0]


lineitem_card = con.sql("SELECT count(*) FROM lineitem").fetchone()[0]

print('sel_shipdate:', {x: y/lineitem_card for x,y in sel_shipdate.items()})
print('sel_discount', {x: y/lineitem_card for x,y in sel_discount.items()})
print('sel_quantity', {x: y/lineitem_card for x,y in sel_quantity.items()})

con.close()