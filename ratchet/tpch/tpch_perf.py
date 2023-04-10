import duckdb
import argparse
import time

from queries import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", type=str, action="store",
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 
                                 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22'], 
                        help="indicate the query id")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-sf", "--scale_factor", type=int, action="store",
                        help="indicate scale factor of the dataset")

    args = parser.parse_args()

    qid = args.query_name
    thread = args.thread
    sf = "SF" + str(args.scale_factor)

    exec_query = globals()[qid].query

    start = time.perf_counter()

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
        results = db_conn.execute(exec_query).fetchdf()

    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))


if __name__ == "__main__":
    main()
