import sys
import numpy as np
import pandas as pd

from kite import kite
from kite.xrg import xrg
from kite.client import client

if __name__ == "__main__":

	new_schema = [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
	sql = '''select count(*) from "tmp/vector/vector*.parquet"'''
	hosts = ["localhost:7878"]

	kitecli = kite.KiteClient()

	h = []
	try:
		kitecli.host(hosts).sql(sql).schema(new_schema).filespec(kite.ParquetFileSpec()).fragment(-1, 2).submit()

		print("run SQL: ", sql)

		while True:
			iter = kitecli.next_row()
			if iter is None:
				break
			else:
				print("flag=", iter.flags, ", values=", iter.values)

	except OSError as msg:
		print(msg)
	finally:
		kitecli.close()


