import duckdb


def main(): 
    sf = "SF1"
    
    query = f"""
    SELECT  L_RETURNFLAG,
            L_LINESTATUS,
            sum(L_QUANTITY) as SUM_QTY,
            sum(L_EXTENDEDPRICE) as SUM_BASE_PRICE,
            sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as SUM_DISC_PRICE,
            sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as SUM_CHARGE,
            avg(L_QUANTITY) as AVG_QTY,
            avg(L_EXTENDEDPRICE) as AVG_PRICE,
            avg(L_DISCOUNT) as AVG_DISC,
            count(*) as COUNT_ORDER
    FROM    'tpch/parquet/{sf}/lineitem.parquet'
    WHERE   L_SHIPDATE <= '1998-09-16'
    GROUP BY L_RETURNFLAG, L_LINESTATUS
    ORDER BY L_RETURNFLAG, L_LINESTATUS
    """

    query_slim = f"""
    SELECT  sum(L_QUANTITY) as SUM_QTY,
            avg(L_DISCOUNT) as AVG_DISC
    FROM    'tpch/parquet/{sf}/lineitem.parquet'
    """

    cursor = duckdb.connect(database=':memory:')
    results = cursor.execute_ratchet(query_slim, 100).fetchdf()
    # results = cursor.execute(query_slim).fetchdf()

    print(results)


if __name__ == "__main__":
    main()
