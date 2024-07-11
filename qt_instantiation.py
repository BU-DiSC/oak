import psycopg2 
import pandas as pd

import re


def get_column_ranges(columns, cur, tables):
	ranges = {}
	for column in columns:
		for table in tables:
			try:
				cur.execute(f"SELECT MIN({column}), MAX({column}) FROM {table};")
				min_val, max_val = cur.fetchone()
				if min_val is not None and max_val is not None:
					ranges[f"{table}.{column}"] = (min_val, max_val)
					break  # Exit the loop once we have found the range for the column
			except psycopg2.Error as e:
				# Ignore errors (e.g., column does not exist in this table)
				cur.execute("ROLLBACK")  # Rollback the transaction
				continue 	
	return ranges


def parse_template(template):
	# Use regex to find all placeholders in the template
	placeholders = re.findall(r"':\d+'", template)
	# Define a pattern to match column names associated with placeholders
	column_pattern = r"(\w+)\s*(?:BETWEEN\s+':\d+'\s+AND\s+':\d+'|[<>]=?\s*':\d+'|=\s*':\d+')"
	columns = re.findall(column_pattern, template)

	# Define a pattern to extract tables from the FROM clause (comma-separated)
	table_pattern = r'FROM\s+((?:\w+\s*,\s*)*\w+)'
	match = re.search(table_pattern, template, re.IGNORECASE)
	if match:
		tables = match.group(1).split(',')
		tables = [table.strip() for table in tables if table.strip()]
	else:
		tables = []
	
	return placeholders, columns, tables

conn = psycopg2.connect(database="tpch",host="localhost", user = "postgres",password = "postgres", port ="5432")
cursor = conn.cursor()

qt1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= ':1' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"

qt4 = "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= ':1' and o_orderdate < ':2' and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority;"

qt10 = "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= ':1' and o_orderdate < ':2' and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc;"

qt14 = "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= ':1' and l_shipdate < ':2';"

qt15 = "select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= ':1' and l_shipdate < ':2' group by l_suppkey;"

placeholders, columns, tables = parse_template(qt15)
print("Placeholders:", placeholders)
print("Columns:", columns)
print("Tables:", tables)
ranges = get_column_ranges(columns, cursor, tables)
print("Column Ranges:", ranges)
cursor.close()
conn.close()

