import duckdb
import argparse
import time
import pandas as pd

from queries import *


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", type=str, action="store",
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 
                                 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22',
                                 'q23', 'q24', 'q25', 'q26', 'q27', 'q28', 'q29', 'q30', 'q31', 'q32', 'q33',
                                 'q34', 'q35', 'q36', 'q37', 'q38', 'q39', 'q40', 'q41', 'q42', 'q43', 'q44',
                                 'q45', 'q46', 'q47', 'q48', 'q49', 'q50', 'q51', 'q52', 'q53', 'q54', 'q55',
                                 'q56', 'q57', 'q58', 'q59', 'q60', 'q61', 'q62', 'q63', 'q64', 'q65', 'q66',
                                 'q67', 'q68', 'q69', 'q70', 'q71', 'q72', 'q73', 'q74', 'q75', 'q76', 'q77',
                                 'q78', 'q79', 'q80', 'q81', 'q82', 'q83', 'q84', 'q85', 'q86', 'q87', 'q88',
                                 'q89', 'q90', 'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99'],
                        help="indicate the query id")
    parser.add_argument("-d", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for TPC-DS such as <dataset/tbl/sf1>")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-ut", "--update_table", action="store_true",
                        help="force to update table in database")

    args = parser.parse_args()

    qid = args.query
    data_folder = args.data_folder
    thread = args.thread
    update_table = args.update_table

    exec_query = globals()[qid].query

    start = time.perf_counter()

    db_conn = duckdb.connect(database=':memory:')
    db_conn.execute(f"PRAGMA threads={thread}")

    tpch_table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                        "customer_address", "customer_demographics", "date_dim", "dbgen_version",
                        "household_demographics", "income_band", "inventory", "item", "promotion",
                        "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
                        "warehouse", "web_page", "web_returns", "web_sales", "web_site"]

    # Create or Update TPC-H Datasets
    for t in tpch_table_names:
        if update_table:
            db_conn.execute(f"DROP TABLE {t};")
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{data_folder}/{t}.parquet');")

    results = None
    if isinstance(exec_query, list):
        for idx, query in enumerate(exec_query):
            if idx == len(exec_query) - 1:
                results = db_conn.execute(query).fetchdf()
            else:
                db_conn.execute(query)
    else:
        results = db_conn.execute(exec_query).fetchdf()

    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))


if __name__ == "__main__":
    main()
