package com.vitessedata.kite.client;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;

public class SockStream {

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    public SockStream(Socket socket) throws IOException {
        this.socket = socket;
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    public void close() throws IOException {
        in.close();
        out.close();
        socket.close();
    }

    private void readfully(byte[] msg, int msgsz) throws IOException {
        int p = 0;
        int len = msgsz;

        while (p < msgsz) {
            int n = in.read(msg, p, len);
            if (n <= 0) {
                throw new IOException("socket unexpected EOF");
            }
            if (n > 0) {
                p += n;
                len -= n;
                continue;
            }
        }
    }

    public void send(byte[] msgty, byte[] msg) throws IOException {

        int msgsz = 0;
        if (msg != null) {
            msgsz = msg.length;
        }

        String hex = String.format("%08X", msgsz);
        ByteBuffer meta = ByteBuffer.allocate(12);
        meta.put(msgty);
        meta.put(hex.getBytes());

        out.write(meta.array());
        if (msg != null && msg.length > 0) {
            out.write(msg);
        }
    }

    public KiteMessage recv() throws IOException {

        ByteBuffer meta = ByteBuffer.allocate(12);
        readfully(meta.array(), meta.array().length);

        byte[] msgty = new byte[4];
        byte[] hex = new byte[8];

        meta.get(msgty);
        meta.get(hex);

        int msglen = Integer.parseInt(new String(hex), 16);

        KiteMessage msg = new KiteMessage(msgty, msglen);
        byte[] buf = msg.getMessageBuffer();
        readfully(buf, msglen);

        return msg;
    }

}
