SELECT sum(l_extendedprice), count(l_extendedprice), l_linestatus, l_returnflag FROM "lineitem*" GROUP BY 3, 4
