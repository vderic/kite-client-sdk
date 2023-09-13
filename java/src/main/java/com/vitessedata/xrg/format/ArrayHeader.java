package com.vitessedata.xrg.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class ArrayHeader {
    public static final int HEADER_SIZE = 16;

    public int len; /* length of the buffer */
    public int ndim; /* # of dimensions same as postgres. */
    public int dataoffset; /* offset of data, or 0 if no bitmap */
    public short ptyp; /* physical type of the array */
    public short ltyp; /* logical type of the array */

    public ArrayHeader() {

    }

    public int getSize() {
        return len;
    }

    public void setSize(int len) {
        this.len = len;
    }

    public int getNDim() {
        return ndim;
    }

    public void setNDim(int ndim) {
        this.ndim = ndim;
    }

    public int getDataOffset() {
        return dataoffset;
    }

    public void setDataOffset(int dataoffset) {
        this.dataoffset = dataoffset;
    }

    public boolean hasNull() {
        return (dataoffset != 0);
    }

    public short getPhysicalType() {
        return ptyp;
    }

    public void setPhysicalType(short ptyp) {
        this.ptyp = ptyp;
    }

    public short getLogicalType() {
        return ltyp;
    }

    public void setLogicalType(short ltyp) {
        this.ltyp = ltyp;
    }

    public static ArrayHeader read(ByteBuffer from, ArrayHeader hdr) {
        int len = from.getInt();
        int ndim = from.getInt();
        int dataoffset = from.getInt();
        short ptyp = from.getShort();
        short ltyp = from.getShort();

        hdr.setSize(len);
        hdr.setNDim(ndim);
        hdr.setDataOffset(dataoffset);
        hdr.setPhysicalType(ptyp);
        hdr.setLogicalType(ltyp);
        return hdr;
    }
}
