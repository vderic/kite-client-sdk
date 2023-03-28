package com.vitessedata.kite.client;

import java.lang.System;

public class KiteMessage {

    public static final byte[] KIT1 = { 'K', 'I', 'T', '1' };
    public static final byte[] JSON = { 'J', 'S', 'O', 'N' };
    public static final byte[] BYE = { 'B', 'Y', 'E', '_' };
    public static final byte[] ERROR = { 'E', 'R', 'R', '_' };
    public static final byte[] VECTOR = { 'V', 'E', 'C', '_' };

    private byte[] buffer = null;
    private int msglen = 0;
    private byte[] msgty = null;

    public KiteMessage(byte[] msgty, int msglen) {
        this.msgty = new byte[4];
        System.arraycopy(msgty, 0, this.msgty, 0, this.msgty.length);
        buffer = new byte[msglen];
        this.msglen = msglen;

    }

    public byte[] getMessageType() {
        return msgty;
    }

    public int getMessageLength() {
        return msglen;
    }

    public byte[] getMessageBuffer() {
        return buffer;
    }
}
