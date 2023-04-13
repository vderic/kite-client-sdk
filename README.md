# Kite Client SDK

Kite Client SDK allows query the data stored in Kite server.  We provide both C and Java interfaces to connect with Kite.

For C, read the file`c/xcli.c` for usage.

```
  hdl = kite_submit(host, schema, sql, fragid, fragcnt, &fs, errmsg, errlen);
  if (!hdl) {
    fprintf(stderr, "kite_submit failed. %s", errmsg);
    return 1;
  }

  e = 0;
  while (e == 0) {
    xrg_iter_t *iter = 0;
    e = kite_next_row(hdl, &iter, errmsg, errlen);
    if (e == 0) {
      // data here
      if (iter == NULL) {
        // no more row
        fprintf(stderr, "EOF nrow = %d\n", nrow);
        break;
      }
    } else {
      fprintf(stderr, "error %d nrow = %d\n. (reason = %s)", e, nrow, errmsg);
    }
  }
```

For Java, read the `java/src/main/java/com/vitessedata/kite/sdk/KiteCmd.java` for usage.

```
        KiteConnection kite = null;
        try {
            kite = new KiteConnection();
            kite.host(addr).schema(schemafn).sql(sqlfn).format(new CsvFileSpec()).fragment(0, 2);
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
                    System.out.print(objs[i].toString());
                    if (objs[i] instanceof BigDecimal) {
                        bigdec = bigdec.add((BigDecimal) objs[i]);
                    } else if (objs[i] instanceof BigInteger) {
                        bigint = bigint.add((BigInteger) objs[i]);
                    }
                }
                System.out.println();
            }
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
```




