import sys
import socket
import client

if __name__ == '__main__':
	
	print("hi")
	msg = client.KiteMessage(client.KiteMessage.KIT1, 10)
	msg.print()

	msgsz = 1035
	hex = f"{msgsz:0{8}X}"
	meta = client.KiteMessage.KIT1 + hex.encode(encoding = 'UTF-8')
	print(meta)
	msgty = meta[0:4]
	msgsz = int(bytearray(meta[4:]), 16)

	print(msgty)
	print(msgsz)

