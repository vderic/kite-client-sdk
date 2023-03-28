package com.vitessedata.xrg.format;

public class Util {

    public static int xrg_align(int alignment, int value) {
        return (value + alignment - 1) & ~(alignment - 1);
    }
}
