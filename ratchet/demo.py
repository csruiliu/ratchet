import duckdb
import argparse
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store", required=True,
                        choices=['slim', 'mid'],
                        help="indicate the query id")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-st", "--suspend_time", type=int, action="store",
                        help="indicate pause time point (second)")
    parser.add_argument("-sp", "--suspend_probability", type=float, action="store",
                        help="indicate pause time point (second)")
    args = parser.parse_args()

    qid = args.query_name
    thread = args.thread
    suspend_time = args.suspend_time
    suspend_prob = args.suspend_probability

    # open and connect a database
    # db_conn = duckdb.connect(database=':memory:')
    db_conn = duckdb.connect(database='demo.db')
    db_conn.execute(f"PRAGMA threads={thread}")

    # Copy the TPC-H Datasets
    tpch_table_names = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]
    tpch_dataset_path = "tpch/dataset/parquet/sf1"
    for t in tpch_table_names:
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{tpch_dataset_path}/{t}.parquet');")

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
                    AND CAST(O_ORDERDATE AS DATE) < '1995-01-01'
                    AND C_ACCTBAL > 100
                    AND L_QUANTITY > 5
        """
    else:
        raise ValueError("Query is not supported in demo")

    # start the query execution and count the time
    start = time.perf_counter()
    if suspend_time is None:
        results = db_conn.execute(exec_query).fetchdf()
    else:
        results = db_conn.execute_ratchet(exec_query, suspend_time, suspend_prob).fetchdf()
    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))


if __name__ == "__main__":
    main()
