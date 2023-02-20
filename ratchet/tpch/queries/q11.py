query = """
SELECT	PS_PARTKEY,
		sum(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM	'parquet/SCALEFACTOR/partsupp.parquet',
		'parquet/SCALEFACTOR/supplier.parquet',
		'parquet/SCALEFACTOR/nation.parquet'
WHERE	PS_SUPPKEY = S_SUPPKEY
		AND S_NATIONKEY = N_NATIONKEY
		AND N_NAME = 'GERMANY'
GROUP BY	PS_PARTKEY HAVING
			sum(PS_SUPPLYCOST * PS_AVAILQTY) > (
    			SELECT	sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001
    			FROM	'parquet/SCALEFACTOR/partsupp.parquet',
						'parquet/SCALEFACTOR/supplier.parquet',
						'parquet/SCALEFACTOR/nation.parquet'
    			WHERE	PS_SUPPKEY = S_SUPPKEY
						AND S_NATIONKEY = N_NATIONKEY
						AND N_NAME = 'GERMANY'
			)
ORDER BY	VALUE DESC
"""
