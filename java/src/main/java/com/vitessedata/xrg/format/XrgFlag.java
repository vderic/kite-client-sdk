package com.vitessedata.xrg.format;

public class XrgFlag {
    private static final byte XRG_SHIFT_NULL = 0;
    private static final byte XRG_SHIFT_INVAL = 1;
    private static final byte XRG_SHIFT_EXCEPT = 2;

    public static final byte NULL = (1 << XRG_SHIFT_NULL);
    public static final byte INVAL = (1 << XRG_SHIFT_INVAL);
    public static final byte EXCEPT = (1 << XRG_SHIFT_EXCEPT);
}
