package com.vitessedata.kite.sdk;

public class CsvFileSpec extends FileSpec {

    public char delim = ',';
    public char quote = '"';
    public char escape = '"';
    public boolean header_line = false;
    public String nullstr = "";

    public CsvFileSpec() {
        super("csv");
    }

    public CsvFileSpec(char delim, char quote, char escape, boolean header_line, String nullstr) {
        super("csv");
        this.delim = delim;
        this.quote = quote;
        this.escape = escape;
        this.header_line = header_line;
        this.nullstr = nullstr;
    }

    public CsvFileSpec delim(char delim) {
        this.delim = delim;
        return this;
    }

    public CsvFileSpec quote(char quote) {
        this.quote = quote;
        return this;
    }

    public CsvFileSpec escape(char escape) {
        this.escape = escape;
        return this;
    }

    public CsvFileSpec header_line(boolean headerline) {
        this.header_line = header_line;
        return this;
    }

    public CsvFileSpec nullstr(String nullstr) {
        this.nullstr = nullstr;
        return this;
    }
}
