from dataclasses import dataclass
#from PyByteBuffer import ByteBuffer
#from bytebuffer import ByteBuffer
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

@dataclass
class AraryHeader:
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
	list = None

	def __init__(self, bytes, precision, scale):
		self.precision = precision
		self.scale = scale
		self.header = ArrayHeader.read(buf[0:XRG_ARRAY_HEADER_SIZE])

		ndim = self.header.ndim
		if ndim == 0:
			list = []
			return

		if ndim != 1:
			raise ValueError("array is not 1-D")

		pos = XRG_ARRAY_HEADER_SIZE
		dims = np.frombuffer(bytes, dtype=np.int32, count=ndim, offset=pos)
		pos += ndim * 4
		dims = np.frombuffer(bytes, dtype=np.int32, count=ndim, offset=pos)
		pos += ndim * 4
			
		nitems = self.get_nitems(ndim, dims)

		dataoffset = self.header.dataoffset

		hdrsz = 0
		if dataoffset == 0:
			hdrsz = self.getOverHeadNoNulls(ndim)
		else:
			bitmapsz = (nitems + 7) / 8
			bitmap = bytes[pos:pos+bitmapsz]
			hdrsz = self.getOverHeadWithNulls(ndim, nitems)

		# skip to the end of header
		pos = hdrsz
		# read 1-D array data

	def get_nitems(self, ndim, dims):
		return 0 if ndims == 0 else dims[0]

	def getOverHeadNoNUlls(ndim):
		return xrg_align(8, XRG_ARRAY_HEADER_SIZE + 2 * 4 * ndim)

	def getOverHeadWithNulls(ndim, nitems):
		return xrg_align(8, XRG_ARRAY_HEADER_SIZE + 2 * 4 * ndim + ((nitems + 7) / 8))

	def isnull(offset):
		if bitmap is None:
			return False

		if bitmap[offset / 8] & (1 << (offset % 8)) != 0:
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

#	@staticmethod
#	def read(buf):
#		bb = ByteBuffer.wrap(buf)
#		if buf[0:4] != XRG_MAGIC:
#			raise Exception("Invalid data. XRG MAGIC not match")
#
#		bb.get(4)
#		ptyp = bb.get(2, 'little')
#		ltyp = bb.get(2, 'little')
#		fieldidx = bb.get(2, 'little')
#		itemsz = bb.get(2, 'little')
#		scale = bb.get(2, 'little')
#		precision = bb.get(2, 'little')
#		nbyte = bb.get(4, 'little')
#		zbyte = bb.get(4, 'little')
#		nnull = bb.get(4, 'little')
#		nitem = bb.get(4, 'little')
#		ninval = bb.get(4, 'little')
#		bb.get(4)
#		bb.get(8)
#
#		return VectorHeader(ptyp, ltyp, fieldidx, itemsz, scale, precision, nbyte, zbyte, nnull, nitem, ninval)
			
#	@staticmethod
#	def readX(bytes):
#		buf = ByteBuffer.wrap(bytes)
#		magic = buf.get_bytes(4)
#		if magic != XRG_MAGIC:
#			raise ValueError('Invalid XRG_MAGIC')
#
#		ptyp = buf.get_SLInt16()
#		ltyp = buf.get_SLInt16()
#		fieldidx = buf.get_SLInt16()
#		itemsz = buf.get_SLInt16()
#		scale = buf.get_SLInt16()
#		precision = buf.get_SLInt16()
#		nbyte = buf.get_SLInt32()
#		zbyte = buf.get_SLInt32()
#		nnull = buf.get_SLInt32()
#		nitem = buf.get_SLInt32()
#		ninval = buf.get_SLInt32()
#		buf.get_bytes(4)
#		buf.get_bytes(8)
#
#		return VectorHeader(ptyp, ltyp, fieldidx, itemsz, scale, precision, nbyte, zbyte, nnull, nitem, ninval)

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

		
	def is_compressed(self):
		return self.header.zbyte != self.header.nbyte


if __name__ == "__main__":

	data = b'XRG1\x04\x00\x01\x00\x00\x00\x08\x00\x00\x00\x00\x00(\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

	#hdr = VectorHeader.read(data)
	#print(hdr)
	#hdr = VectorHeader.read(ByteBuffer.wrap(bytearray(data)))

	v = Vector(bytearray(data))
	print(v.header)

	print(Flag.NULL)
	print(Flag.INVAL)
	print(Flag.EXCEPT)

	a = np.array([1,2,3])
	print(a)

