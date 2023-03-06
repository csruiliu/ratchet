query = """
SELECT  S_ACCTBAL
        S_NAME,
        N_NAME,
        P_PARTKEY,
        P_MFGR,
        S_ADDRESS,
        S_PHONE,
        S_COMMENT
FROM    'TPCH_DATAPATH/part.parquet',
        'TPCH_DATAPATH/supplier.parquet',
        'TPCH_DATAPATH/partsupp.parquet',
        'TPCH_DATAPATH/nation.parquet',
        'TPCH_DATAPATH/region.parquet'
WHERE   P_PARTKEY = PS_PARTKEY
        AND S_SUPPKEY = PS_SUPPKEY
        AND P_SIZE = 15
        AND P_TYPE LIKE '%BRASS'
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'EUROPE'
        AND PS_SUPPLYCOST = (
                SELECT  min(PS_SUPPLYCOST)
                FROM    'TPCH_DATAPATH/partsupp.parquet',
                        'TPCH_DATAPATH/supplier.parquet',
                        'TPCH_DATAPATH/nation.parquet',
                        'TPCH_DATAPATH/region.parquet'
                WHERE   P_PARTKEY = PS_PARTKEY
                        AND S_SUPPKEY = PS_SUPPKEY
                        AND S_NATIONKEY = N_NATIONKEY
                        AND N_REGIONKEY = R_REGIONKEY
                        AND R_NAME = 'EUROPE'
                )
ORDER BY  S_ACCTBAL desc,
          N_NAME,
          S_NAME,
          P_PARTKEY
LIMIT 100
"""
