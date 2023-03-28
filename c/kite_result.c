#define _POSIX_C_SOURCE 200112L

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "kite_result.h"
#include "lz4.h"
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

int xrg_vector_fill_sockbuf(xrg_vector_t **c, sockbuf_t *sockbuf) {
  xrg_vector_t *v = 0;
  *c = 0;

  v = (xrg_vector_t *)sockbuf->buf;
  if (xrg_vector_is_compressed(v)) {
    int ret = 0;
    int newsz =
        xrg_align(16, sizeof(v->header) + v->header.nbyte + v->header.nitem);

    char *retbuf = malloc(newsz);
    char *buf = retbuf;
    int zbyte = v->header.zbyte;
    int nbyte = v->header.nbyte;
    int nitem = v->header.nitem;

    memcpy(buf, v, sizeof(v->header));
    buf += sizeof(v->header);

    ret = LZ4_decompress_safe(XRG_VECTOR_DATA(v), buf, zbyte, nbyte);

    if (ret != nbyte) {
      fprintf(stderr,
              "xrg_vector_fill_sockbuf: LZ4 decompress_safe byte not match (%d "
              "!= %d)\n",
              ret, nbyte);
      return 0;
    }

    buf += nbyte;
    memcpy(buf, XRG_VECTOR_FLAG(v), nitem);

    *c = (xrg_vector_t *)retbuf;
    (*c)->header.zbyte = (*c)->header.nbyte;
  } else {
    char *buf = malloc(sockbuf->msgsz);
    memcpy(buf, sockbuf->buf, sockbuf->msgsz);
    *c = (xrg_vector_t *)buf;
  }

  return (*c)->header.nitem;
}

/* kite client */
int socket_connect(char *host) {
  struct addrinfo hints, *res;
  int sockfd;
  char newhost[strlen(host) + 1];
  strcpy(newhost, host);

  char *port = strchr(newhost, ':');
  if (port == NULL) {
    fprintf(stderr, "kite: host should be in hostname:port format. %s", host);
    return -1;
  }

  *port = 0;
  port++;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if (0 != getaddrinfo(newhost, port, &hints, &res)) {
    fprintf(stderr, "kite: getaddrinfo error. host = %s, port = %s", newhost,
            port);
    return -1;
  }

  sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd < 0) {
    fprintf(stderr, "kite: cannot create socket");
    return -1;
  }

  if (-1 == connect(sockfd, res->ai_addr, res->ai_addrlen)) {
    fprintf(stderr, "kite: cannot open kite connection");
    return -1;
  }

  freeaddrinfo(res);

  return sockfd;
}

int kite_exec(sockstream_t *ss, const char *json) {

  if (sockstream_send(ss, "KIT1", 0, 0)) {
    fprintf(stderr, "%s", sockstream_errmsg(ss));
    return 1;
  }

  if (sockstream_send(ss, "JSON", strlen(json), json)) {
    fprintf(stderr, "%s", sockstream_errmsg(ss));
    return 2;
  }

  return 0;
}

kite_result_t *kite_get_result(sockstream_t *ss) {
  kite_result_t *res = 0;

  res = malloc(sizeof(kite_result_t));
  memset(res, 0, sizeof(kite_result_t));
  kite_result_fill(ss, res);

  if (res->ncol == 0) {
    kite_result_destroy(res);
    return 0;
  }
  return res;
}

static void kite_result_cols_realloc(kite_result_t *res, int newidx) {
  if (res->cols == 0) {
    res->cols_size = 32;
    res->cols = malloc(sizeof(xrg_vector_t *) * res->cols_size);
    if (!res->cols)
      abort();
  } else if (newidx >= res->cols_size) {
    res->cols_size *= 2;
    res->cols = realloc(res->cols, res->cols_size * sizeof(xrg_vector_t *));
    if (!res->cols)
      abort();
  }
}

bool kite_result_fill(sockstream_t *ss, kite_result_t *res) {
  int i = 0, ret = 0;
  int32_t nrow = 0;
  sockbuf_t sockbuf;
  bool success = false;
  res->ncol = 0;
  res->nrow = 0;

  sockbuf_init(&sockbuf);
  /* fill all columns */
  do {
    if (sockstream_recv(ss, &sockbuf)) {
      const char *errmsg = sockstream_errmsg(ss);
      fprintf(stderr, "sockstream_recv failure. %s", errmsg);
      goto done;
    }

    if (memcmp(sockbuf.msgty, "BYE_", 4) == 0) {
      success = true;
      goto done;
    }

    if (memcmp(sockbuf.msgty, "ERR_", 4) == 0) {
      fprintf(stderr, "%s", sockbuf.buf);
      goto done;
    }

    if (memcmp(sockbuf.msgty, "VEC_", 4) != 0) {
      fprintf(stderr, "VEC_ expected");
      goto done;
    }

    if (sockbuf.msgsz == 0) {
      break;
    }

    if (sockbuf.msgsz > 0) {
      xrg_vector_t *c = 0;
      ret = xrg_vector_fill_sockbuf(&c, &sockbuf);
      if (nrow == 0) {
        nrow = ret;
      }

      if (nrow != ret) {
        fprintf(stderr,
                "VECTOR: number of rows not match between columns (nrow=%d, "
                "ret=%d)",
                nrow, ret);
        goto done;
      }

      kite_result_cols_realloc(res, i);

      res->cols[i] = c;

      i++;
    }

  } while (true);

  res->ncol = i;
  res->nrow = nrow;

done:
  sockbuf_final(&sockbuf);
  return success;
}

int kite_result_get_nfields(kite_result_t *res) { return res->ncol; }

int kite_result_get_nrow(kite_result_t *res) { return res->nrow; }

xrg_iter_t *kite_result_next(kite_result_t *res) {
  char errmsg[1024];

  if (res->ncol <= 0) {
    return 0;
  }

  if (!res->iter) {
    res->iter = xrg_iter_create(res->ncol, (const xrg_vector_t **)res->cols,
                                errmsg, sizeof(errmsg));
    if (!res->iter) {
      fprintf(stderr, "xrg_iter_create failed: errmsg = %s", errmsg);
      return 0;
    }
  }
  if (xrg_iter_next(res->iter)) {
    return 0;
  }
  return res->iter;
}

void kite_result_destroy(kite_result_t *res) {
  if (res) {
    if (res->iter) {
      xrg_iter_release(res->iter);
      res->iter = 0;
    }
    if (res->cols) {
      int i = 0;
      for (i = 0; i < res->ncol; i++) {
        free(res->cols[i]);
        res->cols[i] = 0;
      }
      free(res->cols);
    }
    free(res);
  }
}
