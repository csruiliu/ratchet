import duckdb
import argparse
import time
import pandas as pd


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)

    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store", required=True,
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
            SELECT  N_NAME, R_NAME
            FROM  	nation,
                    region
            WHERE	N_REGIONKEY = R_REGIONKEY
        """
    elif qid == "join-2":
        exec_query = f"""
            SELECT  C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS
            FROM  	customer,
                    orders
            WHERE	C_CUSTKEY = O_CUSTKEY
        """
    elif qid == "join-3":
        exec_query = f"""
            SELECT  P_NAME, PS_AVAILQTY, S_ACCTBAL
            FROM  	partsupp,
                    part,
                    supplier
            WHERE	PS_PARTKEY = P_PARTKEY
                    AND	PS_SUPPKEY = S_SUPPKEY
        """
    elif qid == "join-4":
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
