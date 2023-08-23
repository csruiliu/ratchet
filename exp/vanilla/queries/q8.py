query = """
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