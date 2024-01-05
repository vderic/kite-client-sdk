import sys
import socket


# KiteMessage
class KiteMessage:
	"""Kite Message class"""
	
	KIT1   = b'KIT1'
	JSON   = b'JSON'
	BYE    = b'BYE_'
	ERROR  = b'ERR_'
	VECTOR = b'VEC_'

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
			n = self.socket.recv_into(memoryview(msg)[p:], msglen)
			#print("read = ", n, " msgsz =", msgsz, "\nmsg = ", msg)
			if n <= 0:
				raise Exception("scoket unexpected EOF")

			if n > 0:
				p += n
				msglen -= n
				continue

		#print("recv", msg)

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
		msglen = int(meta[4:], 16)
		msg = KiteMessage(msgty, msglen)
		if msglen > 0:
			self.readfully(msg.buffer, msglen)
		return msg
