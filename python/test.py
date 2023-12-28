import json
import copy
import selectors
import socket
import sys
import numpy as np
import pandas as pd

from kite import kite
from kite.client import client
from kite.xrg import xrg

if __name__ == "__main__":

    new_schema = [('id', 'int64'), ('embedding', 'float[]', 0, 0)]
    sql = '''select id, embedding <#> '{9,3,5}' from "tmp/vector/vector*.csv"'''
    hosts = ["localhost:7878"]

    columns = [c[0] for c in new_schema]

    kitecli = kite.KiteClient()
    try:
        kitecli.host(hosts).sql(sql).schema(new_schema).filespec(kite.CsvFileSpec()).fragment(-1, 2).submit()

        print("run SQL: ", sql)

        while True:
            batch = kitecli.next_batch()
            if batch is None:
                break
            else:
                #print(arr)
                df = pd.DataFrame(batch).transpose()
                print("Result")
                print(df)
                df.sort_values(by=df.columns[1], ascending=False, inplace=True)
                df2 = df.sort_values(by=df.columns[1], ascending=False)

                print("Sort By Simliary Score")
                print(df2)

                #df3 = pd.concat([df, df2])
                #df3.sort_values(by=df.columns[1], ascending=False, inplace=True)
                #print(df3)



                print("NBest 3")
                nbest = df2.head(3)
                print(nbest)
                #print(type(row.dtypes))


    except OSError as msg:
        print(msg)
    finally:
        print("END")
        kitecli.close()


