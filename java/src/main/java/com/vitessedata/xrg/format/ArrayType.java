package com.vitessedata.xrg.format;

import java.lang.RuntimeException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ShortBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;

public class ArrayType {

    private byte[] buffer;
    private ArrayHeader header;
    private int[] dims;
    private int[] lbs;
    private byte[] bitmap;
    private byte[] data;

    private ArrayList list;

    public ArrayType(byte[] b, int precision, int scale) {

        init(b);
    }

    private void init(byte[] b) {
        buffer = b;
        ByteBuffer bbuf = ByteBuffer.wrap(b);
        bbuf.order(ByteOrder.LITTLE_ENDIAN);
        header = ArrayHeader.read(bbuf, new ArrayHeader());
        int ndim = header.getNDim();
        if (ndim == 0) {
            // create empty list
            list = new ArrayList();
            return;
        }

        if (ndim != 1) {
            // error
            throw new RuntimeException("array is not 1D");
        }

        dims = new int[ndim];
        lbs = new int[ndim];
        // get dims
        for (int i = 0; i < ndim; i++) {
            dims[i] = bbuf.getInt();
        }
        // lbound
        for (int i = 0; i < ndim; i++) {
            lbs[i] = bbuf.getInt();
        }

        int nitems = getNItems(ndim, dims);
        int dataoffset = header.getDataOffset();
        int hdrsz = 0;
        if (dataoffset == 0) {
            // no null and calcuate the NONULL header size
            // read dims, lbs
            hdrsz = getOverHeadNoNulls(ndim);

        } else {
            // has null and calculate the WITHNULL header size
            // bitmap
            int bitmapsz = (nitems + 7) / 8;
            bitmap = new byte[bitmapsz];
            bbuf.get(bitmap);
            hdrsz = getOverHeadWithNulls(ndim, nitems);
        }

        // skip to the end of header
        bbuf.get(hdrsz - 1);
        ByteBuffer databuf = bbuf.slice();

        switch (header.getPhysicalType()) {
        case PhysicalTypes.INT8:
            readArray(databuf.asIntBuffer());
            break;
        case PhysicalTypes.INT16:
            readArray(databuf.asShortBuffer());
            break;
        case PhysicalTypes.INT32:
            readArray(databuf.asIntBuffer());
            break;
        case PhysicalTypes.INT64:
            readArray(databuf.asLongBuffer());
            break;
        case PhysicalTypes.INT128:
            readArray(databuf);
            break;
        case PhysicalTypes.FP32:
            readArray(databuf.asFloatBuffer());
            break;
        case PhysicalTypes.FP64:
            readArray(databuf.asDoubleBuffer());
            break;
        case PhysicalTypes.BYTEA:
            readArray(databuf);
            break;
        default:
            throw new RuntimeException("array type not supported");
        }
    }

    public int getNItems(int ndims, int[] dims) {
        return (ndims == 0 ? 0 : dims[0]);
    }

    public int getOverHeadNoNulls(int ndims) {
        return Util.xrg_align(8, ArrayHeader.HEADER_SIZE + 2 * 4 * ndims);
    }

    public int getOverHeadWithNulls(int ndims, int nitems) {
        return Util.xrg_align(8, ArrayHeader.HEADER_SIZE + 2 * 4 * ndims + (nitems + 7) / 8);
    }

    public boolean isnull(int offset) {
        if (bitmap == null) {
            return false;
        }

        if ((bitmap[offset / 8] & (1 << (offset % 8))) != 0) {
            return false;
        }
        return true;
    }

    public Object[] toArray() {
        return list.toArray();
    }

    private void readArray(ByteBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }

    private void readArray(ShortBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }

    private void readArray(IntBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }

    private void readArray(LongBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }

    private void readArray(FloatBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }

    private void readArray(DoubleBuffer databuf) {
        int ndim = header.getNDim();
        int nitems = getNItems(ndim, dims);
        list = new ArrayList(nitems);

        for (int i = 0; i < nitems; i++) {
            if (isnull(i)) {
                list.add(null);
            } else {
                list.add(databuf.get());
            }
        }
    }
}
