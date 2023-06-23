import duckdb
import argparse
import time
import pandas as pd

from queries import *


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)

    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store", required=True,
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9'],
                        help="indicate the query id")
    parser.add_argument("-d", "--database", type=str, action="store", required=True, default="memory",
                        help="indicate the database location, memory or other location")
    parser.add_argument("-df", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for conversion such as <tpch/dataset/parquet/sf1>")
    parser.add_argument("-ut", "--update_table", action="store_true",
                        help="force to update table in database")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")

    parser.add_argument("-s", "--suspend_query", action="store_true", default=False,
                        help="whether it is a suspend query")
    parser.add_argument("-st", "--suspend_start_time", type=float, action="store",
                        help="indicate start time for suspension (second)")
    parser.add_argument("-se", "--suspend_end_time", type=float, action="store",
                        help="indicate end time for suspension (second)")
    parser.add_argument("-sf", "--suspend_file", type=str, action="store",
                        help="indicate the file for suspending query")

    parser.add_argument("-r", "--resume_query", action="store_true", default=False,
                        help="whether it is a resumed query")
    parser.add_argument("-rf", "--resume_file", type=str, action="store",
                        help="indicate the file for resuming query")

    args = parser.parse_args()

    qid = args.query_name
    database = args.database
    data_folder = args.data_folder
    thread = args.thread
    suspend_query = args.suspend_query
    resume_query = args.resume_query
    update_table = args.update_table

    exec_query = globals()[qid].query

    if suspend_query:
        suspend_start_time = args.suspend_start_time
        suspend_end_time = args.suspend_end_time
        suspend_file = args.suspend_file

    if resume_query:
        resume_file = args.resume_file

    # open and connect a database
    if database == "memory":
        db_conn = duckdb.connect(database=':memory:')
    else:
        db_conn = duckdb.connect(database=database)

    duck_tmp = "/home/ruiliu/Develop/ratchet-duckdb/ratchet/tmp"
    db_conn.execute(f"PRAGMA temp_directory='{duck_tmp}'")
    db_conn.execute(f"PRAGMA threads={thread}")

    tpch_table_names = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

    # Create or Update TPC-H Datasets
    for t in tpch_table_names:
        if update_table:
            db_conn.execute(f"DROP TABLE {t};")
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{data_folder}/{t}.parquet');")

    # start the query execution
    if suspend_query:
        results = db_conn.execute_suspend(exec_query, suspend_file, suspend_start_time, suspend_end_time).fetchdf()
    elif resume_query:
        results = db_conn.execute_resume(exec_query, resume_file).fetchdf()
    else:
        results = db_conn.execute(exec_query).fetchdf()

    print(results)
    db_conn.close()


if __name__ == "__main__":
    main()
