SELECT sum(l_extendedprice), count(l_extendedprice), l_linestatus, l_returnflag FROM "vd-s3-tmp/test_tpch/lineitem*" GROUP BY 3, 4
