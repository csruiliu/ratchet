import duckdb
import argparse
import time
import pandas as pd


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)

    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store", required=True,
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
    parser.add_argument("-r", "--resume_query", action="store_true",
                        help="whether it is a resumed query")
    args = parser.parse_args()

    qid = args.query_name
    data_folder = args.data_folder
    thread = args.thread
    suspend_start_time = args.suspend_start_time
    suspend_end_time = args.suspend_end_time
    update_table = args.update_table
    resume_query = args.resume_query

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
    if qid == "sum-1":
        exec_query = f"""
            SELECT  sum(L_QUANTITY) as SUM_QTY
            FROM    lineitem
        """
    elif qid == "avg-1":
        exec_query = f"""
            SELECT  avg(L_DISCOUNT) as AVG_DISC
            FROM    lineitem
        """
    elif qid == "orderby-1":
        exec_query = f"""
            SELECT  sum(L_QUANTITY) as REVENUE
            FROM    lineitem
            ORDER BY    REVENUE
        """
    elif qid == "join-1":
        exec_query = f"""
            SELECT  C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS
            FROM  	customer,
                    orders
            WHERE	C_CUSTKEY = O_CUSTKEY
        """
    elif qid == "join-2":
        exec_query = f"""
            SELECT  P_NAME, PS_AVAILQTY, S_ACCTBAL
            FROM  	partsupp,
                    part,
                    supplier
            WHERE	PS_PARTKEY = P_PARTKEY
                    AND	PS_SUPPKEY = S_SUPPKEY
        """
    elif qid == "join-3":
        exec_query = f"""
            SELECT  C_CUSTKEY, O_ORDERKEY, L_LINENUMBER, L_QUANTITY, C_ACCTBAL, O_TOTALPRICE
            FROM  	customer,
                    orders,
                    lineitem
            WHERE	C_CUSTKEY = O_CUSTKEY
                    AND	L_ORDERKEY = O_ORDERKEY
                    AND CAST(O_ORDERDATE AS DATE) >= '1994-01-01'
                    AND C_ACCTBAL > 100
                    AND L_QUANTITY > 5
        """
    elif qid == "join-groupby-orderby-1":
        exec_query = f"""
            SELECT  L_ORDERKEY, sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as REVENUE, O_ORDERDATE, O_SHIPPRIORITY
            FROM    orders,
                    lineitem
            WHERE	L_ORDERKEY = O_ORDERKEY
                    AND CAST(O_ORDERDATE AS DATE) >= '1994-01-01'
            GROUP BY    L_ORDERKEY,
                        O_ORDERDATE,
                        O_SHIPPRIORITY
            ORDER BY    O_ORDERDATE
        """
    elif qid == "groupby-orderby-1":
        exec_query = f"""
            SELECT  L_ORDERKEY, sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as REVENUE
            FROM    lineitem
            GROUP BY    L_ORDERKEY
            ORDER BY    REVENUE
        """
    else:
        raise ValueError("Query is not supported in demo")

    if resume_query:
        results = db_conn.execute_resume(exec_query, "demo.ratchet").fetchdf()
    else:
        if suspend_start_time is not None and suspend_end_time is not None:
            results = db_conn.execute_suspend(exec_query, "demo.ratchet", suspend_start_time, suspend_end_time).fetchdf()
        else:
            results = db_conn.execute(exec_query).fetchdf()

    print(results)
    db_conn.close()


if __name__ == "__main__":
    main()
