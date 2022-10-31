#ifndef _KITESDK_H_
#define _KITESDK_H_

#include "xrg.h"
#include <stdbool.h>

typedef struct kite_handle_t kite_handle_t;

#ifdef __cplusplus
extern "C" {
#endif

/* submit query to kite */
kite_handle_t *kite_submit(char *addr, const char *schema, const char *sql,
                           int fragid, int fragcnt, char *errmsg, int errlen);

/*
 * get the next row from socket
 * if error return -1 error,
 * if success return 0,
 * if no more pending data return 1
 * */
int kite_next_row(kite_handle_t *, xrg_iter_t **retiter, char *errmsg,
                  int errlen);

/* destroy the client */
void kite_release(kite_handle_t *);

#ifdef __cplusplus
}
#endif

#endif
