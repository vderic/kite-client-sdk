import json
import copy
import selectors
import socket
import sys
import numpy as np
import pandas as pd

from kite.client import client
from kite.xrg import xrg

class FileSpec:
	fmt = None

	def __init__(self, fmt):
		self.fmt = fmt

class CsvFileSpec(FileSpec):

	delim = ','
	quote = '"'
	escape = '"'
	header_line = False
	nullstr = ""

	def __init__(self, delim = ',', quote = '"',escape = '"', header_line = False, nullstr = ""):
		super().__init__("csv")
		self.delim = delim
		self.quote = quote
		self.escape = escape
		self.header_line = header_line
		self.nullstr = nullstr


	def toJSON(self):
		fs = {}
		fs["fmt"] = "csv"
		fs["csvspec"] = {"delim": self.delim, "quote": self.quote, "escape": self.escape, 
						 "header_line": ("true" if self.header_line else "false"), 
						 "nullstr": self.nullstr}
		return fs

class ParquetFileSpce(FileSpec):

	def __init__(self):
		super().__init__("parquet")
	
	def toJSON(self):
		fs = {}
		fs["fmt"] = "parquet"
		return fs



class Request:

	_schema = None
	_sql = None
	_fragid = 0
	_fragcnt = 0
	_filespec = None

	def __init__(self):
		return

	def schema(self, _schema):
		self._schema = _schema

	def sql(self, sql):
		self._sql = sql

	def fragment(self, fragid, fragcnt):
		self._fragid = fragid
		self._fragcnt = fragcnt

	def filespec(self, fs):
		self._filespec = fs

	def schema2json(self):
		array = []

		for column in self._schema:
			if len(column) != 2 and len(column) != 4:
				raise Exception("schema format error")

			cname = column[0]
			ty = column[1]
		
			# check type name
			if ty not in xrg.LogicalTypes.TYPES:
				raise ValueError("input type is invalid. type = " + ty)

			if ty == 'decimal' or ty == 'decimal[]':
				if len(column) != 4:
					raise Exception("schema format error. decimal needs name, type, precision and scale")
				precision = column[2]
				scale = column[3]

				array.append({"name": cname, "type": ty, "precision": precision, "scale": scale})
			else:
				array.append({"name": cname, "type": ty})

		return array

	def toJSON(self):
		ret = {}

		if self._sql is None:
			raise Exception("sql not defined yet")

		if self._schema is None:
			raise Exception("schema not defined yet")

		if self._fragcnt == 0 or self._fragid >= self._fragcnt:
			raise Exception("fragment not defined yet")

		if self._filespec is None:
			raise Exception("filespec not defined yet")

		ret["sql"] = self._sql
		ret["fragment"] = [self._fragid, self._fragcnt]
		ret["schema"] = self.schema2json()
		ret["filespec"] = self._filespec.toJSON()

		return json.dumps(ret)

	def print(self):
		print(self._schema)
		print(self._sql)
		print(self._fragid)
		print(self._fragcnt)

class KiteClient:

	hosts = []
	port = 0
	fragid = 0
	fragcnt = 0
	req = None
	selectors = selectors.DefaultSelector()
	sockstreams = []

	batches = []
	curr = None

	def __init__(self):
		self.req = Request()

	def host(self, addresses):
		for addr in addresses:
			hostport = addr.split(':', 2)
			if len(hostport) != 2:
				raise Exception('address should be host:port')

			host = hostport[0]
			port = int(hostport[1])
			self.hosts.append((host, port))

		return self
	
	def schema(self, schema):
		self.req.schema(schema)
		return self

	def sql(self, sql):
		self.req.sql(sql)
		return self

	def fragment(self, fragid, fragcnt):
		self.fragid = fragid
		self.fragcnt = fragcnt
		return self

	def filespec(self, fs):
		self.req.filespec(fs)
		return self

	def print(self):
		print(self.hosts)
		self.req.print()
		
	def connect(self, addr):
		#print(addr)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(addr)
		#sock.setblocking(False)
		return sock
	

	def read(self, ss, mask):
		row = []

		while True:
			msg = ss.recv()
			#print("type= ", msg.msgty, " , len=", msg.msglen)
			if msg.msgty == client.KiteMessage.BYE:
				return None
			elif msg.msgty == client.KiteMessage.ERROR:
				raise Exception(msg.buffer[0:msg.msglen].decode('utf-8'))
			elif msg.msgty == client.KiteMessage.VECTOR:
				if msg.msglen == 0:
					break
				else:
					vec = xrg.Vector(msg.buffer)
					row.append(vec)
					# print(vec.values)
			else:
				raise Exception("Invalid Kite message type")

		#print("read ncol ", len(row))
		return row

	def submit(self):

		requests = []

		if self.fragcnt <= 0:
			raise Exception("error: fragcnt <= 0")

		if self.fragid == -1:
			for i in range(self.fragcnt):
				fragr = copy.deepcopy(self.req)
				fragr.fragment(i, self.fragcnt)
				requests.append(fragr)
		else:
			self.req.fragment(self.fragid, self.fragcnt)
			requests.append(self.req)
	
		#for r in requests:
		#	print(r.toJSON())

		# connect to server
		for i in range(len(requests)):
			n = i % len(self.hosts)
			sock = self.connect(self.hosts[n])
			if sock is None:
				raise Exception("could not open socket")
			ss = client.SockStream(sock)
			self.sockstreams.append(ss)
	
		# selector
		for ss in self.sockstreams:
			self.selectors.register(ss, selectors.EVENT_READ, self.read)
	
		# send message
		for r, ss in zip(requests, self.sockstreams):
			json = None
			ss.send(client.KiteMessage.KIT1, None)
			ss.send(client.KiteMessage.JSON, r.toJSON().encode('utf-8'))
	
	
	def next_batch(self):

		if len(self.batches) != 0:
			return self.batches.pop()

		while True:
			if len(self.sockstreams) == 0:
				break
				
			events = self.selectors.select(None)

			for key, mask in events:
				callback = key.data
				page = callback(key.fileobj, mask)
				if page is None:
					self.selectors.unregister(key.fileobj)
					try:
						key.fileobj.close()
					except OSError as msg:
						print(msg)

					self.sockstreams.remove(key.fileobj)
				else:
					# push to the list
					# return XrgIterator
					self.batches.append(xrg.XrgIterator(page))

		# check the stack for any vector found and return
		#print("try to get one row and return")
		if len(self.batches) == 0:
			return None
		
		return self.batches.pop()
			

	def next_row(self):

		if self.curr is not None and self.curr.has_next():
			return self.curr.next()

		self.curr = self.next_batch()
		if self.curr is not None and self.curr.has_next():
			return self.curr.next()
		return self.curr


	def close(self):
		for ss in self.sockstreams:
			ss.close()

		self.selectors.close()
