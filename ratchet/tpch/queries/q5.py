query = """
SELECT  N_NAME,
        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM  	'parquet/SCALEFACTOR/customer.parquet',
		'parquet/SCALEFACTOR/orders.parquet',
		'parquet/SCALEFACTOR/lineitem.parquet',
		'parquet/SCALEFACTOR/supplier.parquet',
		'parquet/SCALEFACTOR/nation.parquet',
		'parquet/SCALEFACTOR/region.parquet'
WHERE	C_CUSTKEY = O_CUSTKEY
		AND	L_ORDERKEY = O_ORDERKEY
		AND L_SUPPKEY = S_SUPPKEY
		AND C_NATIONKEY = S_NATIONKEY
		AND S_NATIONKEY = N_NATIONKEY
		AND N_REGIONKEY = R_REGIONKEY
		AND R_NAME = 'ASIA'
		AND CAST(O_ORDERDATE AS DATE) >= '1994-01-01'
		AND CAST(O_ORDERDATE AS DATE) < '1995-01-01'
GROUP BY  N_NAME
ORDER BY  REVENUE desc
"""
