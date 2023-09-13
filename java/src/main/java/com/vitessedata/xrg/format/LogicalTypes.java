package com.vitessedata.xrg.format;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class LogicalTypes {

    public static final List TYPES = Arrays.asList("int8", "int16", "int32", "int64", "float", "double", "decimal",
            "string", "interval", "time", "date", "timestamp", "int8[]", "int16[]", "int32[]", "int64[]", "float[]",
            "double[]", "decimal[]", "string[]", "interval[]", "time[]", "date[]", "timestamp[]");
    public static final short UNKNOWN = 0;
    public static final short NONE = 1;
    public static final short STRING = 2;
    public static final short DECIMAL = 3;
    public static final short INTERVAL = 4;
    public static final short TIME = 5;
    public static final short DATE = 6;
    public static final short TIMESTAMP = 7;
    public static final short ARRAY = 8;
}
