package com.vitessedata.kite.math;

import java.lang.Math;

public class DecimalUtil {

    public static final int MAX_DEC128_PRECISION = 38;

    public static int[] getPrecisionScaleforADD(int p1, int s1, int p2, int s2) {
        int precision, scale;
        precision = scale = 0;
        // scale = max(s1, s2);
        // precision = max(p1-s1, p2-s2) + 1 + scale
        // assert(p1 != 0 && p2 != 0);
        scale = Math.max(s1, s2);
        precision = Math.max(p1 - s1, p2 - s2) + 1 + scale;

        if (precision > MAX_DEC128_PRECISION) {
            throw new RuntimeException("Decimal ADD/SUB: result precision out of range. precision = " + precision);
        }

        return new int[] { precision, scale };
    }

    public static int[] getPrecisionScaleforSUB(int p1, int s1, int p2, int s2) {
        return getPrecisionScaleforADD(p1, s1, p2, s2);
    }

    public static int[] getPrecisionScaleforDIV(int p1, int s1, int p2, int s2) {
        int precision, scale;
        precision = scale = 0;
        // scale = max(4, s1 + p2 - s2 + 1)
        // precision = p1 - s1 + s2 + scale
        // assert(p1 != 0 && p2 != 0);
        scale = Math.max(4, s1 + p2 - s2 + 1);
        precision = p1 - s1 + s2 + scale;

        if (precision > MAX_DEC128_PRECISION) {
            throw new RuntimeException("Decimal DIV: result precision out of range. precision = " + precision);
        }
        return new int[] { precision, scale };
    }

    public static int[] getPrecisionScaleforMUL(int p1, int s1, int p2, int s2) {
        int precision, scale;
        precision = scale = 0;
        // scale = s1 + s2
        // precision = precision = p1 + p2 + 1
        // assert(p1 != 0 && p2 != 0);
        scale = s1 + s2;
        precision = p1 + p2 + 1;
        if (precision > MAX_DEC128_PRECISION) {
            throw new RuntimeException("Decimal MUL: result precision out of range. precision = " + precision);
        }
        return new int[] { precision, scale };
    }

    public static int[] getPrecisionScaleforMOD(int p1, int s1, int p2, int s2) {

        int p = Math.max(p1, p2);
        int s = Math.max(s1, s2);
        return new int[] { p, s };
    }
}
