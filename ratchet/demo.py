import duckdb
import argparse
import time

from tpch.queries import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query_name", type=str, action="store",
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 'q12', 'q13',
                                 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22', 'slim', 'mid'],
                        help="indicate the query id")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-sf", "--scale_factor", type=int, action="store", help="indicate scale factor of the dataset")
    parser.add_argument("-pt", "--pause_time", type=int, action="store", help="indicate pause time point (second)")
    args = parser.parse_args()

    qid = args.query_name
    thread = args.thread
    suspend_time = args.pause_time
    sf = "SF" + str(args.scale_factor)

    if qid == "slim":
        exec_query = f"""
            SELECT  avg(L_DISCOUNT) as AVG_DISC,
                    sum(L_QUANTITY) as SUM_QTY
            FROM    'TPCH_DATAPATH/lineitem.parquet'
        """
    elif qid == "mid":
        exec_query = f"""
            SELECT  count(L_EXTENDEDPRICE) AS REVENUE
            FROM  	'TPCH_DATAPATH/customer.parquet',
                    'TPCH_DATAPATH/orders.parquet',
                    'TPCH_DATAPATH/lineitem.parquet',
            WHERE	C_CUSTKEY = O_CUSTKEY
                    AND	L_ORDERKEY = O_ORDERKEY
                    AND CAST(O_ORDERDATE AS DATE) >= '1994-01-01'
                    AND CAST(O_ORDERDATE AS DATE) < '1995-01-01'
        """
    else:
        exec_query = globals()[qid].query

    start = time.perf_counter()
    # db_conn = duckdb.connect(database=':memory:', config={"threads": 2})
    db_conn = duckdb.connect(database=':memory:')
    db_conn.execute(f"PRAGMA threads={thread}")

    results = None
    query_len = len(exec_query) - 1
    if isinstance(exec_query, list):
        for idx, query in enumerate(exec_query):
            if idx == query_len:
                results = db_conn.execute(query.replace("TPCH_DATAPATH", f"tpch/parquet/{sf}")).fetchdf()
            else:
                db_conn.execute(query.replace("TPCH_DATAPATH", f"tpch/parquet/{sf}"))
    else:
        exec_query = exec_query.replace("TPCH_DATAPATH", f"tpch/parquet/{sf}")
        results = db_conn.execute_ratchet(exec_query, suspend_time).fetchdf()
        # results = db_conn.execute(exec_query).fetchdf()

    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))


if __name__ == "__main__":
    main()
