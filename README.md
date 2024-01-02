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

        while ((iter = kite.next()) != null) {
            Object[] objs = iter.getValues();
            for (int i = 0; i < objs.length; i++) {
                if (i > 0) {
                    System.out.print("|");
                }
                System.out.print(objs[i].toString());
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


For Python,

Install lz4, pandas, numpy package first,

```
% pip3 install pandas
% pip3 install numpy
% pip3 intall lz4
```

Install the package

```
% git clone git@github.com:vderic/kite-client-sdk.git
% cd kite-client-sdk/python
% pip3 install .
```

For development in Python 3,
```
        kitecli = kite.KiteClient()
        try:
                kitecli.host(hosts).sql(sql).schema(new_schema).filespec(kite.CsvFileSpec()).fragment(-1, 2).submit()

                while True:
                        iter = kitecli.next_row()
                        if iter is None:
                                break
                        else:
                                print("flag=", iter.flags, ", values=", iter.values)

        except OSError as msg:
                print(msg)
        finally:
                kitecli.close()
```

