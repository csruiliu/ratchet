import duckdb
import argparse
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store", required=True,
                        choices=['slim', 'mid'],
                        help="indicate the query id")
    parser.add_argument("-d", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for conversion such as <tpch/dataset/parquet/sf1>")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-st", "--suspend_start_time", type=float, action="store",
                        help="indicate start time for suspension (second)")
    parser.add_argument("-se", "--suspend_end_time", type=float, action="store",
                        help="indicate end time for suspension (second)")
    parser.add_argument("-u", "--update_table", action="store_true",
                        help="force to update table in database")
    args = parser.parse_args()

    qid = args.query_name
    data_folder = args.data_folder
    thread = args.thread
    suspend_start_time = args.suspend_start_time
    suspend_end_time = args.suspend_end_time
    update_table = args.update_table

    # open and connect a database
    # db_conn = duckdb.connect(database=':memory:')
    db_conn = duckdb.connect(database='demo.db')
    db_conn.execute(f"PRAGMA threads={thread}")

    tpch_table_names = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

    # Create or Update TPC-H Datasets
    for t in tpch_table_names:
        if update_table:
            db_conn.execute(f"DROP TABLE {t};")
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{data_folder}/{t}.parquet');")

    # start the query execution
    if qid == "slim":
        exec_query = f"""
            SELECT  avg(L_DISCOUNT) as AVG_DISC,
                    sum(L_QUANTITY) as SUM_QTY
            FROM    lineitem
        """
    elif qid == "mid":
        exec_query = f"""
            SELECT  C_CUSTKEY, O_ORDERKEY, L_LINENUMBER, L_QUANTITY, C_ACCTBAL
            FROM  	customer,
                    orders,
                    lineitem
            WHERE	C_CUSTKEY = O_CUSTKEY
                    AND	L_ORDERKEY = O_ORDERKEY
                    AND CAST(O_ORDERDATE AS DATE) >= '1994-01-01'
                    AND C_ACCTBAL > 100
                    AND L_QUANTITY > 5
        """
    else:
        raise ValueError("Query is not supported in demo")

    # start the query execution and count the time
    start = time.perf_counter()
    if suspend_start_time is not None and suspend_end_time is not None:
        results = db_conn.execute_suspend(exec_query, "demo.ratchet", suspend_start_time, suspend_end_time).fetchdf()
    else:
        results = db_conn.execute(exec_query).fetchdf()
    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))
    db_conn.execute_resume(exec_query, "demo.ratchet")
    db_conn.close()


if __name__ == "__main__":
    main()
