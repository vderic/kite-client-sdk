import json
import copy
import selectors
import socket
import sys
from client import client

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

		lines = self._schema.split('\n')
		for l in lines:
			column = l.split(':', 4)
			if len(column) != 2 and len(column) != 4:
				raise Exception("schema format error")

			cname = column[0]
			ty = column[1]
		
			# TODO: check type name

			if ty == 'decimal' or ty == 'decimal[]':
				if len(column) != 4:
					raise Exception("schema format error. decimal needs name, type, precision and scale")
				precision = int(column[2])
				scale = int(column[3])

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
		sock = None
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except OSError as msg:
			print(msg)
			sock = None
			return sock
	
		try:
			print(addr)
			sock.connect(addr)
			#sock.setblocking(False)
		except OSError as msg:
			print(msg)
			sock.close()
			sock = None
		return sock
	

	def read(self, ss, mask):
		print("read callback")
		msg = None
		msg = ss.recv()
		print("type= ", msg.msgty, " , len=", msg.msglen)

		if msg.msgty == b'BYE_':
			print("BYE BYE")
			ss.close()
			return None

		return msg

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
	

		for r in requests:
			print(r.toJSON())

		# connect to server
		for i in range(len(requests)):
			n = i % len(self.hosts)
			sock = self.connect(self.hosts[n])
			if sock is None:
				print('could not open socket')
				sys.exit(1)
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
	
	
	def get_next(self):
		events = self.selectors.select(None)
		for key, mask in events:
			callback = key.data
			return callback(key.fileobj, mask)


	def close(self):
		for ss in self.sockstreams:
			ss.close()

		self.selectors.close()
		
		
if __name__ == "__main__":
	schema = 'id:int64:0:0\nembedding:float[]:0:0'
	sql = 'select * from "tmp/vector/vector*.csv"'
	hosts = ["localhost:7878"]

	kite = KiteClient()
	try:
		kite.host(hosts).sql(sql).schema(schema).filespec(CsvFileSpec()).fragment(-1, 2).submit()

		while True:
			msg = kite.get_next()
			if msg is None:
				break
				
	except OSError as msg:
		print(msg)

	
	kite.close()
	


