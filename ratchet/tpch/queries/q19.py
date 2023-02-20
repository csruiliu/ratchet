query = """
SELECT	sum(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE
FROM	'parquet/SCALEFACTOR/lineitem.parquet',
		'parquet/SCALEFACTOR/part.parquet'
wHERE	(
			P_PARTKEY = L_PARTKEY
			AND P_BRAND = 'Brand#12'
			AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
			AND L_QUANTITY >= 1 AND L_QUANTITY <= 11
			AND P_SIZE BETWEEN 1 AND 5
			AND L_SHIPMODE IN ('AIR', 'AIR REG')
			AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
		)
		OR
		(
			P_PARTKEY = L_PARTKEY
			AND P_BRAND = 'Brand#23'
			AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
			AND L_QUANTITY >= 10 AND L_QUANTITY <= 20
			AND P_SIZE BETWEEN 1 AND 10
			AND L_SHIPMODE IN ('AIR', 'AIR REG')
			AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
		)
		OR
		(
			P_PARTKEY = L_PARTKEY
			AND P_BRAND = 'Brand#34'
			AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
			AND L_QUANTITY >= 20 AND L_QUANTITY <= 30
			AND P_SIZE BETWEEN 1 AND 15
			AND L_SHIPMODE IN ('AIR', 'AIR REG')
			AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
		)
"""
