package com.vitessedata.xrg.format;

import java.io.InputStream;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.lang.RuntimeException;
import java.lang.StringBuffer;

public class XrgVectorHeader {
    public static final int HEADER_SIZE = 48;
    private static final byte[] XRG_MAGIC = { 'X', 'R', 'G', '1' };

    private short ptyp = 0;
    private short ltyp = 0;
    private short fieldidx = 0;
    private short itemsz = 0;
    private short scale = 0;
    private short precision = 0;
    private int nbyte = 0;
    private int zbyte = 0;
    private int nnull = 0;
    private int nitem = 0;
    private int ninval = 0;

    public XrgVectorHeader() {

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

    public short getFieldIdx() {
        return fieldidx;
    }

    public void setFieldIdx(short fieldidx) {
        this.fieldidx = fieldidx;
    }

    public short getItemSize() {
        return itemsz;
    }

    public void setItemSize(short itemsz) {
        this.itemsz = itemsz;
    }

    public short getScale() {
        return scale;
    }

    public void setScale(short scale) {
        this.scale = scale;
    }

    public short getPrecision() {
        return precision;
    }

    public void setPrecision(short precision) {
        this.precision = precision;
    }

    public int getNByte() {
        return nbyte;
    }

    public void setNByte(int nbyte) {
        this.nbyte = nbyte;
    }

    public int getZByte() {
        return zbyte;
    }

    public void setZByte(int zbyte) {
        this.zbyte = zbyte;
    }

    public int getNNull() {
        return nnull;
    }

    public void setNNull(int nnull) {
        this.nnull = nnull;
    }

    public int getNItem() {
        return nitem;
    }

    public void setNItem(int nitem) {
        this.nitem = nitem;
    }

    public int getNInval() {
        return ninval;
    }

    public void setNInval(int ninval) {
        this.ninval = ninval;
    }

    public static XrgVectorHeader read(ByteBuffer from, XrgVectorHeader vechdr) {
        byte[] magic = new byte[4];
        from.get(magic);
        if (Arrays.equals(XRG_MAGIC, magic) == false) {
            throw new RuntimeException("Wrong Magic");
        }

        // read 48 bytes header
        short ptyp = from.getShort();
        short ltyp = from.getShort();
        short fieldidx = from.getShort();
        short itemsz = from.getShort();
        short scale = from.getShort();
        short precision = from.getShort();
        int nbyte = from.getInt();
        int zbyte = from.getInt();
        int nnull = from.getInt();
        int nitem = from.getInt();
        int ninval = from.getInt();
        from.getInt(); // unused int32
        from.getLong(); // unseud int64

        vechdr.setPhysicalType(ptyp);
        vechdr.setLogicalType(ltyp);
        vechdr.setFieldIdx(fieldidx);
        vechdr.setItemSize(itemsz);
        vechdr.setScale(scale);
        vechdr.setPrecision(precision);
        vechdr.setNByte(nbyte);
        vechdr.setZByte(zbyte);
        vechdr.setNNull(nnull);
        vechdr.setNItem(nitem);
        vechdr.setNInval(ninval);
        return vechdr;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("ptyp =");
        sb.append(ptyp);
        sb.append(",");
        sb.append("ltyp =");
        sb.append(ltyp);
        sb.append(",");
        sb.append("precision=");
        sb.append(precision);
        sb.append(",");
        sb.append("scale=");
        sb.append(scale);
        sb.append(",");
        sb.append("nbyte=");
        sb.append(nbyte);
        sb.append(",");
        sb.append("zbyte=");
        sb.append(zbyte);
        sb.append(",");
        sb.append("nitem=");
        sb.append(nitem);
        sb.append(",");
        sb.append("itemsz=");
        sb.append(itemsz);
        return sb.toString();
    }

}
