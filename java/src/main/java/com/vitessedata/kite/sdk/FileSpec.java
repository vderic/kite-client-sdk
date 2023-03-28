package com.vitessedata.kite.sdk;

import java.io.Serializable;

public class FileSpec implements java.io.Serializable {
    public String fmt;

    public FileSpec(String fmt) {
        this.fmt = fmt;
    }
}
