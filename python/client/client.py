import sys
import socket


# KiteMessage
class KiteMessage:
	"""Kite Message class"""
	
	KIT1   = b'KIT1'
	JSON   = b'JSON'
	BYE    = b'BYT_'
	ERROR  = b'ERR_'
	VECTOR = b'VEC_'

	buffer = None
	msglen = 0
	msgty = None

	def __init__(self, msgty, msglen):
		self.msgty = msgty
		self.msglen = msglen
		self.buffer = bytearray(msglen)

	def getMessageType(self):
		return self.msgty

	def getMessageLength(self):
		return self.msglen

	def getMessageBuffer(self):
		return self.buffer

	def print(self):
		print(self.msgty)
		print(self.buffer)

# SockStream
class SockStream:
	"""SockStream"""

	socket = None

	def __init__(self, sock):
		self.socket = sock

	# return socket fileno for select.select()
	def fileno(self):
		return self.socket.fileno()

	def close(self):
		self.socket.close()

	def readfully(self, msg, msgsz):
		p = 0
		msglen = msgsz

		while p < msgsz:
			n = self.socket.recv_into(msg[p:], msglen)
			if n <= 0:
				raise Exception("scoket unexpected EOF")

			if n > 0:
				p += n
				msglen -= n
				continue


	def send(self, msgty, msg):
		msgsz = 0
		if msg is not None:
			msgsz = len(msg)

		hex = f"{msgsz:0{8}X}"
		meta = msgty + hex.encode(encoding = 'UTF-8')

		self.socket.sendall(meta)
		if msg is not None:
			self.socket.sendall(msg)

	def recv(self):
		meta = bytearray(12)
		self.readfully(meta, len(meta))

		msgty = meta[0:4]
		hex = meta[4:]
		msglen = int(hex, 16)
		msg = KiteMessage(msgty, msglen)
		self.readfully(msg.buffer, msglen)


if __name__ == '__main__':
	
	print("hi")
	msg = KiteMessage(KiteMessage.KIT1, 10)
	msg.print()

	msgsz = 1035
	hex = f"{msgsz:0{8}X}"
	meta = KiteMessage.KIT1 + hex.encode(encoding = 'UTF-8')
	print(meta)
	msgty = meta[0:4]
	msgsz = int(meta[4:], 16)

	print(msgty)
	print(msgsz)

