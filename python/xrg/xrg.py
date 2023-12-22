from dataclasses import dataclass
import lz4.frame
import numpy as np

XRG_HEADER_SIZE = 48
XRG_MAGIC = b'XRG1'
XRG_ARRAY_HEADER_SIZE = 16

def xrg_align(alignment, value):
	return (value + alignment - 1) & ~(alignment - 1);

class Flag:
	SHIFT_NULL = 0
	SHIFT_INVAL = 1
	SHIFT_EXCEPT = 2

	NULL = (1 << SHIFT_NULL)
	INVAL = (1 << SHIFT_INVAL)
	EXCEPT = (1 << SHIFT_EXCEPT)

class LogicalTypes:
	TYPES = ["int8", "int16", "int32", "int64", "float", "double", "decimal", 
			"string", "interval", "time", "date", "timestamp",
			"int8[]", "int16[]", "int32[]", "int64[]", "float[]", "double[]", "decimal[]", 
			"string[]", "interval[]", "time[]", "date[]", "timestamp[]"]
	UNKNOWN = 0
	NONE = 1
	STRING = 2
	DECIMAL = 3
	INTERVAL = 4
	TIME = 5
	DATE = 6
	TIMESTAMP = 7
	ARRAY = 8

class PhysicalTypes:
	UNKNOWN = 0
	INT8 = 1
	INT16 = 2
	INT32 = 3
	INT64 = 4
	INT128 = 5
	FP32 = 6
	FP64 = 7
	BYTEA = 8
	
	
@dataclass
class ArrayHeader:
	length: int
	ndim: int
	dataoffset: int
	ptyp: int
	ltyp: int
	
	@staticmethod
	def read(bytes):
		pos = 0
		length = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		ndim = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		dataoffset = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		ptyp = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		ltyp = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2

		hdr = ArrayHeader(length, ndim, dataoffset, ptyp, ltyp)
		return hdr

class ArrayType:
	precision = 0
	scale = 0
	header = None

	dims = None
	lbs = None
	values = None
	bitmap = None

	def __str__(self):
		return " ".join(str(x) for x in self.values)

	def __init__(self, bytes, precision, scale):
		self.precision = precision
		self.scale = scale
		self.header = ArrayHeader.read(bytes[0:XRG_ARRAY_HEADER_SIZE])

		ndim = self.header.ndim
		if ndim == 0:
			values = []
			return

		if ndim != 1:
			raise ValueError("array is not 1-D")

		pos = XRG_ARRAY_HEADER_SIZE
		dims = np.frombuffer(bytes, dtype=np.int32, count=ndim, offset=pos)
		pos += ndim * 4
		lbs = np.frombuffer(bytes, dtype=np.int32, count=ndim, offset=pos)
		pos += ndim * 4
			
		nitems = self.get_nitems(ndim, dims)

		dataoffset = self.header.dataoffset

		hdrsz = 0
		if dataoffset == 0:
			hdrsz = self.getOverHeadNoNulls(ndim)
		else:
			bitmapsz = (nitems + 7) / 8
			self.bitmap = bytes[pos:pos+bitmapsz]
			hdrsz = self.getOverHeadWithNulls(ndim, nitems)

		# skip to the end of header
		pos = hdrsz
		# read 1-D array data
		match self.header.ptyp:
			case PhysicalTypes.INT8:
				self.read_array(bytes, np.int8, nitems, pos, 1)
			case PhysicalTypes.INT16:
				self.read_array(bytes, np.int16, nitems, pos, 2)
			case PhysicalTypes.INT32:
				self.read_array(bytes, np.int32, nitems, pos, 4)
			case PhysicalTypes.INT64:
				self.read_array(bytes, np.int64, nitems, pos, 8)
			case PhysicalTypes.FP32:
				self.read_array(bytes, np.float32, nitems, pos, 4)
			case PhysicalTypes.FP64:
				self.read_array(bytes, np.float64, nitems, pos, 8)
			case PhysicalTypes.BYTEA:
				self.read_string_array(bytes, nitems, pos)
			case _:
				raise ValueError("invalid array data")
				

	def read_array(self, bytes, dtype, nitems, pos, itemsz):
		arr = []
		for i in range(nitems):
			if self.isnull(i):
				arr.append(None)
			else:
				arr.append(np.frombuffer(bytes, dtype=dtype, count=1, offset=pos)[0])
				pos += itemsz
		self.values = np.array(arr, dtype=dtype)

	def read_string_array(self, bytes, nitem, pos):
		if self.header.ltyp != LogicalTypes.STRING:
			raise ValueError("bytea is not string")

		arr = []
		for i in range(nitems):
			if self.isnull(i):
				arr.append(None)
			else:
				sz = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
				pos += 4
				if sz == 0:
					array.append("")
				else:
					arr.append(bytes[pos:pos+sz].decode('utf-8'))
					pos += sz
		self.values = arr

	def get_nitems(self, ndim, dims):
		return 0 if ndim == 0 else dims[0]

	def getOverHeadNoNulls(self, ndim):
		return xrg_align(8, XRG_ARRAY_HEADER_SIZE + 2 * 4 * ndim)

	def getOverHeadWithNulls(self, ndim, nitems):
		return xrg_align(8, XRG_ARRAY_HEADER_SIZE + 2 * 4 * ndim + ((nitems + 7) / 8))

	def isnull(self, offset):
		if self.bitmap is None:
			return False

		if self.bitmap[offset / 8] & (1 << (offset % 8)) != 0:
			return False

		return True
		

@dataclass
class VectorHeader:
	"""xrg vector header"""

	ptyp: int
	ltyp: int
	fieldidx: int
	itemsz: int
	scale: int
	precision: int
	nbyte: int
	zbyte: int
	nnull: int
	nitem: int
	ninval: int

	@staticmethod
	def read(bytes):
		magic = bytes[0:4]
		if magic != XRG_MAGIC:
			raise ValueError('Invalid XRG_MAGIC')

		pos = 4
		ptyp = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		ltyp = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		fieldidx = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		itemsz = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		scale = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		precision = np.frombuffer(bytes, dtype=np.int16, count=1, offset=pos)[0]
		pos += 2
		nbyte = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		zbyte = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		nnull = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		nitem = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4
		ninval = np.frombuffer(bytes, dtype=np.int32, count=1, offset=pos)[0]
		pos += 4

		return VectorHeader(ptyp, ltyp, fieldidx, itemsz, scale, precision, nbyte, zbyte, nnull, nitem, ninval)



class Vector:

	header = None
	data = None
	flag = None
	values = None

	def __init__(self, buf):
		self.header = VectorHeader.read(buf[0:XRG_HEADER_SIZE])
		nbyte = self.header.nbyte
		zbyte = self.header.zbyte
		nitem = self.header.nitem
		
		if self.is_compressed():
			# LZ4
			self.data = lz4.frame.decompress(buf[XRG_HEADER_SIZE:XRG_HEADER_SIZE+zbyte])
		else:
			self.data = buf[XRG_HEADER_SIZE:XRG_HEADER_SIZE+nbyte]

		self.flag = buf[XRG_HEADER_SIZE+zbyte::XRG_HEADER_SIZE+zbyte+nitem]

		match self.header.ptyp:
			case PhysicalTypes.INT8:
				self.values = np.frombuffer(self.data, np.int8, count=nitem)
			case PhysicalTypes.INT16:
				self.values = np.frombuffer(self.data, np.int16, count=nitem)
			case PhysicalTypes.INT32:
				self.values = np.frombuffer(self.data, np.int32, count=nitem)
			case PhysicalTypes.INT64:
				self.values = np.frombuffer(self.data, np.int64, count=nitem)
			case PhysicalTypes.INT128:
				array = []
				pos = 0 
				offset = 0
				for i in range(nitem):
					array.append(self.data[pos:pos+16])
					pos += 16
				self.values = array
			case PhysicalTypes.FP32:
				self.values = np.frombuffer(self.data, np.float32, count=nitem)
			case PhysicalTypes.FP64:
				self.values = np.frombuffer(self.data, np.float64, count=nitem)
			case PhysicalTypes.BYTEA:
				arr = []
				pos = 0
				for i in range(nitem):
					sz = np.frombuffer(self.data, np.int32, count=1, offset=pos)[0]
					pos += 4
					if sz == 0:
						array.append(None)
						continue

					if self.header.ltyp == LogicalTypes.STRING:
						arr.append(self.data[pos:pos+sz].decode('utf-8'))
					elif self.header.ltyp == LogicalTypes.ARRAY:
						#a = ArrayType(self.data[pos:pos+sz], self.header.precision, self.header.scale)
						arr.append(ArrayType(self.data[pos:pos+sz], self.header.precision, self.header.scale).values)
					else:
						arr.append(self.data[pos:pos+sz])
					pos += sz
				self.values = arr
			case _:
				raise ValueError("invalid data type")
				
		for i in range(len(self.flag)):
			if self.flag[i] != 0:
				self.values[i] = None
			
		
	def is_compressed(self):
		return self.header.zbyte != self.header.nbyte


if __name__ == "__main__":

	data = b'XRG1\x04\x00\x01\x00\x00\x00\x08\x00\x00\x00\x00\x00(\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

	arr = b'XRG1\x08\x00\x08\x00\x01\x00\xff\xff\x00\x00\x00\x00\xc8\x00\x00\x00\xc8\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$\x00\x00\x00$\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x06\x00\x01\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x80?\x00\x00\x00@\x00\x00@@$\x00\x00\x00$\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x06\x00\x01\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00@\x00\x00@@\x00\x00\x80@$\x00\x00\x00$\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x06\x00\x01\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x80@\x00\x00\xa0@\x00\x00\xc0@$\x00\x00\x00$\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x06\x00\x01\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x80?\x00\x00\x80@\x00\x00@@$\x00\x00\x00$\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x06\x00\x01\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\xa0@\x00\x00\xe0@\x00\x00@@\x00\x00\x00\x00\x00\x00\x00\x00'

	#hdr = VectorHeader.read(data)
	#print(hdr)
	#hdr = VectorHeader.read(ByteBuffer.wrap(bytearray(data)))

	v = Vector(bytearray(data))
	print(v.header)
	print(v.values)

	a = Vector(bytearray(arr))
	print(a.header)
	for i in a.values:
		print(i)


