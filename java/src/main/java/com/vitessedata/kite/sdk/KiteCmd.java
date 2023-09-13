package com.vitessedata.kite.sdk;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileReader;
import java.lang.StringBuffer;
import org.json.*;
import com.vitessedata.xrg.format.*;
import java.math.BigInteger;
import java.math.BigDecimal;

public class KiteCmd {

    private String sql;
    private String schema;
    private String addr;
    private KiteConnection kite;

    public KiteCmd() {

    }

    public KiteCmd schema(String schemafn) throws IOException {

        JSONTokener tokener = new JSONTokener(new FileReader(schemafn));
        JSONArray array = new JSONArray(tokener);
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);
            String name = obj.getString("name");
            String type = obj.getString("type");
            int precision = 0;
            int scale = 0;
            try {
                precision = obj.getInt("precision");
                scale = obj.getInt("scale");
            } catch (JSONException ex) {
                ;
            }

            sb.append(name);
            sb.append(':');
            sb.append(type);
            sb.append(':');
            sb.append(precision);
            sb.append(':');
            sb.append(scale);
            sb.append('\n');
        }

        schema = sb.toString();
        System.out.println(schema);

        return this;
    }

    public KiteCmd sql(String sqlfn) throws IOException {
        File file = new File(sqlfn);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            sql = new String(data);
            System.out.println("SQL: " + sql);
        }
        return this;
    }

    public KiteConnection create() throws IOException {
        if (sql == null) {
            throw new RuntimeException("sql not defined yet");
        }

        if (schema == null) {
            throw new RuntimeException("schema not defined yet");
        }

        if (addr == null) {
            throw new RuntimeException("address not defined yet");
        }

        kite = new KiteConnection();

        kite.host(addr).sql(sql).schema(schema).format(new CsvFileSpec()).fragment(0, 1);

        return kite;

    }

    public KiteCmd host(String addr) {
        this.addr = addr;
        return this;
    }

    public static void main(String[] args) {

        if (args.length != 3) {
            System.err.println("usage: java KiteCmd hostaddr schema sql");
            return;
        }

        String addr = args[0];
        String schemafn = args[1];
        String sqlfn = args[2];
        KiteCmd cmd = null;
        KiteConnection kite = null;

        try {
            cmd = new KiteCmd();
            kite = cmd.host(addr).schema(schemafn).sql(sqlfn).create();

            kite.submit();
            XrgIterator iter = null;

            BigDecimal bigdec = BigDecimal.ZERO;
            BigInteger bigint = BigInteger.ZERO;

            while ((iter = kite.next()) != null) {
                Object[] objs = iter.getValues();
                for (int i = 0; i < objs.length; i++) {
                    if (i > 0) {
                        System.out.print("|");
                    }
                    if (objs[i] == null) {
                        System.out.print("NULL");
                        continue;
                    }

                    if (objs[i] instanceof ArrayType) {
                        ArrayType arr = (ArrayType) objs[i];
                        Object[] values = arr.toArray();
                        System.out.print("{");
                        for (int j = 0; j < values.length; j++) {
                            if (j > 0) {
                                System.out.print(",");
                            }
                            if (values[j] == null) {
                                System.out.print("NULL");
                            } else {
                                System.out.print(values[j].toString());
                            }
                        }
                        System.out.print("}");
                    } else {
                        System.out.print(objs[i].toString());
                        if (objs[i] instanceof BigDecimal) {
                            bigdec = bigdec.add((BigDecimal) objs[i]);
                        } else if (objs[i] instanceof BigInteger) {
                            bigint = bigint.add((BigInteger) objs[i]);
                        }
                    }
                }
                System.out.println();
            }

            System.out.println("BIGINT= " + bigint + " , BIGDEC = " + bigdec);
        } catch (IOException ex) {
            System.err.println(ex);
        } finally {
            try {
                if (kite != null) {
                    kite.release();
                }
            } catch (IOException ex) {
                ;
            }
        }

    }

}
