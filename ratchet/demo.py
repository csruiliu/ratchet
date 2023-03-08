import duckdb
import argparse
from tpch.queries import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query",
                        type=str,
                        action="store",
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 'q12', 'q13',
                                 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22', 'slim1', 'slim2'],
                        help="indicate the query id")
    parser.add_argument("-sf", "--scale_factor", type=int, action="store", help="indicate scale factor of the dataset")
    args = parser.parse_args()

    qid = args.query
    sf = "SF" + str(args.scale_factor)

    if qid == "slim1":
        exec_query = f"""
            SELECT  avg(L_DISCOUNT) as AVG_DISC
            FROM    'TPCH_DATAPATH/lineitem.parquet'
        """
    else:
        exec_query = globals()[qid].query

    # db_conn = duckdb.connect(database=':memory:', config={"threads": 2})
    db_conn = duckdb.connect(database=':memory:')
    # db_conn.execute("PRAGMA threads=2")

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
        results = db_conn.execute_ratchet(exec_query, 100).fetchdf()

    print(results)


if __name__ == "__main__":
    main()
