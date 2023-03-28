package com.vitessedata.kite.sdk;

import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.Vector;
import com.vitessedata.kite.client.*;
import com.vitessedata.xrg.format.*;

public class KiteConnection {

    private SockStream sockstream;
    private XrgIterator iter;
    private Request req;
    private String host;
    private int port;

    public KiteConnection() {
        sockstream = null;
        iter = null;
        req = new Request();

    }

    public KiteConnection host(String addr) {
        if (!addr.contains(":")) {
            throw new IllegalArgumentException("addr should be host:port");
        }
        String[] addrport = addr.split(":", 2);
        host = addrport[0];
        port = Integer.parseInt(addrport[1]);
        return this;
    }

    public KiteConnection schema(String schema) {
        req.schema(schema);
        return this;
    }

    public KiteConnection fragment(int fragid, int fragcnt) {
        req.fragment(fragid, fragcnt);
        return this;
    }

    public KiteConnection sql(String sql) {
        req.sql(sql);
        return this;
    }

    public KiteConnection format(FileSpec filespec) {
        req.format(filespec);
        return this;
    }

    public KiteConnection request(Request request) {
        req = request;
        return this;
    }

    /*
     * submit a SQL query to kite. addr: host:port schema: lines of "name:type:precision:scale" fragid: a number between
     * 0 and fragnct-1 fragcnt: max number of fragments
     */
    public void submit() throws IOException {

        if (host == null) {
            throw new RuntimeException("host not defined yet");
        }

        Socket socket = new Socket(host, port);
        sockstream = new SockStream(socket);

        String json = req.toJSON().toString();

        sockstream.send(KiteMessage.KIT1, null);

        // System.out.println(json);
        sockstream.send(KiteMessage.JSON, json.getBytes());

    }

    public XrgIterator next() throws IOException {

        for (;;) {
            if (iter == null) {
                Vector<XrgVector> pages = readXrgVector();
                if (pages == null || pages.size() == 0) {
                    return null;
                }
                iter = new XrgIterator(pages);
            }
            if (iter.next()) {
                return iter;
            }
            iter = null;
        }
    }

    public void release() throws IOException {
        sockstream.close();
    }

    private Vector<XrgVector> readXrgVector() throws IOException {
        Vector<XrgVector> pages = new Vector<XrgVector>();

        while (true) {
            KiteMessage msg = sockstream.recv();

            byte[] msgty = msg.getMessageType();
            if (Arrays.equals(msgty, KiteMessage.ERROR)) {
                throw new IOException(new String(msg.getMessageBuffer()));

            } else if (Arrays.equals(msgty, KiteMessage.VECTOR)) {

                int len = msg.getMessageLength();
                if (len == 0) {
                    break;
                } else {
                    XrgVector v = new XrgVector(msg.getMessageBuffer());
                    pages.addElement(v);
                }
            } else if (Arrays.equals(msgty, KiteMessage.BYE)) {
                release();
                break;
            } else {
                throw new IOException("Invalid Kite message type");
            }

        }

        return pages;
    }

}
