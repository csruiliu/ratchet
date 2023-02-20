query = """
SELECT	S_NAME,
	count(*) AS NUMWAIT
FROM	'parquet/SCALEFACTOR/supplier.parquet',
		'parquet/SCALEFACTOR/lineitem.parquet' l1,
		'parquet/SCALEFACTOR/orders.parquet',
		'parquet/SCALEFACTOR/nation.parquet'
WHERE	S_SUPPKEY = l1.L_SUPPKEY
		AND O_ORDERKEY = l1.L_ORDERKEY
		AND O_ORDERSTATUS = 'F'
		AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
		AND EXISTS(
			SELECT	*
			FROM	'parquet/SCALEFACTOR/lineitem.parquet' l2
			WHERE	l2.L_ORDERKEY = l1.L_ORDERKEY
					AND l2.L_SUPPKEY <> l1.L_SUPPKEY
		)
		AND NOT EXISTS(
			SELECT	*
			FROM	'parquet/SCALEFACTOR/lineitem.parquet' l3
			WHERE	l3.L_ORDERKEY = l1.L_ORDERKEY
					AND l3.L_SUPPKEY <> l1.L_SUPPKEY
					AND l3.L_RECEIPTDATE > l3.L_COMMITDATE
		)
		AND S_NATIONKEY = N_NATIONKEY
		AND N_NAME = 'SAUDI ARABIA'
GROUP BY	S_NAME
ORDER BY	NUMWAIT desc,
			S_NAME
LIMIT 100
"""
