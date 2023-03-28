package com.vitessedata.xrg.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class XrgVector {

    private byte[] buffer;
    private XrgVectorHeader header;
    private ByteBuffer data;
    private ByteBuffer flag;

    public XrgVector(byte[] buf) {
        init(buf);
    }

    private void init(byte[] buf) {
        buffer = buf;
        ByteBuffer headerbuf = ByteBuffer.wrap(buf, 0, XrgVectorHeader.HEADER_SIZE);
        headerbuf.order(ByteOrder.LITTLE_ENDIAN);
        header = XrgVectorHeader.read(headerbuf, new XrgVectorHeader());
        int nbyte = header.getNByte();
        int zbyte = header.getZByte();
        int nitem = header.getNItem();

        if (isCompressed()) {
            // decompress the data
            LZ4Factory factory = LZ4Factory.fastestInstance();
            ByteBuffer zbuf = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE, zbyte);
            zbuf.order(ByteOrder.LITTLE_ENDIAN);
            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            data = ByteBuffer.allocate(nbyte);
            data.order(ByteOrder.LITTLE_ENDIAN);
            decompressor.decompress(zbuf, data);
            data.rewind();
        } else {
            data = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE, nbyte);
            data.order(ByteOrder.LITTLE_ENDIAN);
        }
        flag = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE + zbyte, nitem);
        flag.order(ByteOrder.LITTLE_ENDIAN);

    }

    public boolean isCompressed() {
        return (header.getZByte() != header.getNByte());
    }

    public XrgVectorHeader getHeader() {
        return header;
    }

    public ByteBuffer getData() {
        return data;
    }

    public ByteBuffer getFlag() {
        return flag;
    }

}
