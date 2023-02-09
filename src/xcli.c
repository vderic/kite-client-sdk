#ifdef __STDC_ALLOC_LIB__
#define __STDC_WANT_LIB_EXT2__ 1
#else
#define _POSIX_C_SOURCE 200809L
#endif

#include "kitesdk.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *read_file_fully(const char *fn) {

  int n;
  FILE *f = fopen(fn, "rb");
  if (f == 0) {
    return 0;
  }
  fseek(f, 0, SEEK_END);
  long fsize = ftell(f);
  fseek(f, 0, SEEK_SET); /* same as rewind(f); */

  char *string = malloc(fsize + 1);
  if ((n = fread(string, fsize, 1, f)) < 0) {
    return 0;
  }

  fclose(f);

  string[fsize] = 0;
  return string;
}

int main(int argc, const char *argv[]) {
  char *host;
  const char *sqlfn;
  const char *schemafn;
  int e;
  char *sql = 0;
  char *schema = 0;
  int nrow = 0;
  int fragid = 0, fragcnt = 0;
  kite_handle_t *hdl = 0;
  char errmsg[1024];
  int errlen = sizeof(errmsg);

  if (argc != 6) {
    fprintf(stderr, "usage: xcli host1:port1,host2:port2,...,hostN:portN "
                    "schemafn sqlfn fragid fragcnt\n");
    return 1;
  }

  host = strdup(argv[1]);
  schemafn = argv[2];
  sqlfn = argv[3];
  fragid = atoi(argv[4]);
  fragcnt = atoi(argv[5]);

  schema = read_file_fully(schemafn);
  if (!schema) {
    fprintf(stderr, "schema: read file failed\n");
    return 1;
  }

  sql = read_file_fully(sqlfn);

  if (!sql) {
    fprintf(stderr, "sql: read file failed\n");
    return 1;
  }

  kite_filespec_t fs;
  strcpy(fs.fmt, "csv");
  fs.u.csv.delim = ',';
  fs.u.csv.quote = '"';
  fs.u.csv.escape = '"';
  fs.u.csv.header_line = 0;
  *fs.u.csv.nullstr = 0;

  hdl = kite_submit(host, schema, sql, fragid, fragcnt, &fs, errmsg, errlen);
  if (!hdl) {
    fprintf(stderr, "kite_submit failed. %s", errmsg);
    return 1;
  }

  e = 0;
  while (e == 0) {
    xrg_iter_t *iter = 0;
    char errbuf[1024];
    e = kite_next_row(hdl, &iter, errmsg, errlen);
    if (e == 0) {
      // data here
      if (iter == NULL) {
        // no more row
        fprintf(stderr, "EOF nrow = %d\n", nrow);
        break;
      }

      if (iter->curr == 0) {
      	xrg_vector_print(stdout, iter->nvec, (const xrg_vector_t **) iter->vec, '|', "NULL", errbuf, sizeof(errbuf));
      }

      nrow++;
    } else {
      // error handling
      fprintf(stderr, "error %d nrow = %d\n. (reason = %s)", e, nrow, errmsg);
    }
  }

  free(host);
  if (schema) {
    free(schema);
  }
  if (sql) {
    free(sql);
  }
  if (hdl) {
    kite_release(hdl);
  }

  return 0;
}
