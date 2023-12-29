import json
import copy
import selectors
import socket
import sys
import heapq
import numpy as np
import pandas as pd

from kite import kite
from kite.xrg import xrg
from kite.client import client

if __name__ == "__main__":

	new_schema = [('id', 'int64'), ('embedding', 'float[]', 0, 0)]
	sql = '''select embedding <#> '{9,3,5}', id from "tmp/vector/vector*.csv"'''
	hosts = ["localhost:7878"]

	columns = [c[0] for c in new_schema]

	kitecli = kite.KiteClient()

	h = []
	nbest = 3
	try:
		kitecli.host(hosts).sql(sql).schema(new_schema).filespec(kite.CsvFileSpec()).fragment(-1, 2).submit()

		print("run SQL: ", sql)

		while True:
			iter = kitecli.next_row()
			if iter is None:
				break
			else:
				print("flag=", iter.flags, ", values=", iter.values)
				#print(tuple(iter.values))
				if len(h) <= nbest:
					heapq.heappush(h, tuple(iter.values))
				else:
					heapq.heapreplace(h, tuple(iter.values))

		for i in range(len(h)):
			print(heapq.heappop(h))

	except OSError as msg:
		print(msg)
	finally:
		print("END")
		kitecli.close()


