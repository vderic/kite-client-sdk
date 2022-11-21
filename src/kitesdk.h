#ifndef _KITESDK_H_
#define _KITESDK_H_

#include "xrg.h"

#define MAX_FILESPEC_FMT_LEN 8
#define MAX_CSV_NULLSTR_LEN 8

typedef struct kite_handle_t kite_handle_t;

typedef struct kite_filespec_t {
  char fmt[MAX_FILESPEC_FMT_LEN];

  union {
    struct {
      char delim;
      char quote;
      char escape;
      int8_t header_line;
      char nullstr[MAX_CSV_NULLSTR_LEN];
    } csv;
  } u;
} kite_filespec_t;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Submit a SQL query to kite.
 * addr: lines of "host:port".
 * schema: lines of "name:type:precision:scale".
 * fragid: -1 indicate all fragments; otherwise, a number between 0 and
 * fragcnt-1. fragcnt: max number of fragments.
 */
kite_handle_t *kite_submit(char *addr, const char *schema, const char *sql,
                           int fragid, int fragcnt, kite_filespec_t *fs,
                           char *errmsg, int errlen);

/**
 * Get the next row from kite server.
 * Returns 0 on success, -1 otherwise.
 * On EOF, return NULL in retiter.
 */
int kite_next_row(kite_handle_t *, xrg_iter_t **retiter, char *errmsg,
                  int errlen);

/**
 * Release the connection to kite server.
 */
void kite_release(kite_handle_t *);

#ifdef __cplusplus
}
#endif

#endif
