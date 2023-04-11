#ifdef __STDC_ALLOC_LIB__
#define __STDC_WANT_LIB_EXT2__ 1
#else
#define _POSIX_C_SOURCE 200809L
#endif

#include "kitesdk.h"
#include "kite_result.h"
#include "sockstream.h"
#include "stringbuffer.h"
#include <event2/event.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <strings.h>

typedef struct listcell_t listcell_t;
struct listcell_t {
  kite_result_t *arg;
  listcell_t *next;
};

static void listcell_free(listcell_t *lc) {
  if (lc->arg)
    kite_result_destroy(lc->arg);
  free(lc);
}

typedef struct list_t list_t;
struct list_t {
  listcell_t *head;
};

static list_t *list_append(list_t *list, kite_result_t *arg) {

  list_t *ret = 0;
  listcell_t *lc;

  if (list == NULL) {
    ret = (list_t *)malloc(sizeof(list_t));
    ret->head = 0;
  } else {
    ret = list;
  }

  lc = (listcell_t *)malloc(sizeof(listcell_t));
  lc->arg = arg;
  lc->next = 0;

  if (ret->head == NULL) {
    ret->head = lc;
  } else {
    listcell_t *curr = ret->head;
    while (curr->next != NULL) {
      curr = curr->next;
    }
    curr->next = lc;
  }

  return ret;
}

static void list_free(list_t *list) {
  if (list) {
    listcell_t *lc = list->head;
    while (lc) {
      listcell_t *nxt = lc->next;
      listcell_free(lc);
      lc = nxt;
    }

    free(list);
  }
}

static listcell_t *list_pop(list_t *list) {
  if (list) {
    listcell_t *ret = list->head;
    if (list->head) {
      list->head = list->head->next;
    }
    return ret;
  }
  return NULL;
}

typedef struct kite_evcxt_t kite_evcxt_t;
struct kite_evcxt_t {
  sockstream_t *ss;
  void *arg;
  struct event *ev;
};

// typedef struct kite_handle_t kite_handle_t;
struct kite_handle_t {
  struct event_base *evbase;
  kite_evcxt_t *evcxt;
  int nevt;
  list_t *rlist;
  listcell_t *curr_res;
};

static kite_handle_t *kite_handle_create() {
  kite_handle_t *client = (kite_handle_t *)malloc(sizeof(kite_handle_t));
  memset(client, 0, sizeof(kite_handle_t));
  client->evbase = event_base_new();
  return client;
}

void kite_release(kite_handle_t *client) {
  if (client) {

    if (client->evcxt) {
      for (int i = 0; i < client->nevt; i++) {
        if (client->evcxt[i].ss) {
          sockstream_destroy(client->evcxt[i].ss);
        }
        if (client->evcxt[i].ev) {
          event_free(client->evcxt[i].ev);
        }
      }

      free(client->evcxt);
    }

    if (client->evbase) {
      event_base_free(client->evbase);
    }

    list_free(client->rlist);

    if (client->curr_res) {
      listcell_free(client->curr_res);
    }

    free(client);
  }

  libevent_global_shutdown();
}

static void kite_evcb(evutil_socket_t fd, short what, void *arg) {
  (void)fd;
  kite_evcxt_t *cxt = (kite_evcxt_t *)arg;
  kite_handle_t *client = (kite_handle_t *)cxt->arg;
  if (what & EV_TIMEOUT) {
    event_base_loopbreak(((kite_handle_t *)cxt->arg)->evbase);
  } else if (what & EV_READ) {
    kite_result_t *res = kite_get_result(cxt->ss);
    if (!res) {
      event_del(cxt->ev);
      cxt->ev = 0;
      return;
    }

    client->rlist = list_append(client->rlist, res);
  }
}

static int kite_handle_exec(kite_handle_t *client, char **json, int n,
                            char *errmsg, int errlen) {

  int e = 0;

  if (n != client->nevt) {
    fprintf(
        stderr,
        "number of json requests do not match with number of connections\n");
    return 1;
  }
  for (int i = 0; i < client->nevt; i++) {
    e = kite_exec(client->evcxt[i].ss, json[i]);
    if (e) {
      snprintf(errmsg, errlen, "kite_exec error");
      return 1;
    }
  }
  return 0;
}

static int kite_handle_assign_socket(kite_handle_t *client, int *sockfd,
                                     int nsocket, char *errmsg, int errlen) {
  struct event *ev;
  // struct timeval fivesec = {5, 0};

  client->nevt = nsocket;
  client->evcxt = (kite_evcxt_t *)malloc(sizeof(kite_evcxt_t) * nsocket);
  if (!client->evcxt) {
    snprintf(errmsg, errlen, "kite_handle_assign_socket: out of memory");
    return 1;
  }

  for (int i = 0; i < client->nevt; i++) {
    client->evcxt[i].ss = sockstream_assign(sockfd[i]);
    client->evcxt[i].arg = client;
    ev = event_new(client->evbase, sockfd[i],
                   /* EV_TIMEOUT | */ EV_READ | EV_PERSIST, kite_evcb,
                   &client->evcxt[i]);
    client->evcxt[i].ev = ev;
    // event_add(ev, &fivesec);
    event_add(ev, NULL);
  }
  return 0;
}

static int kite_handle_connect(kite_handle_t *client, char **hosts,
                               int nconnection, char *errmsg, int errlen) {

  int sockfd[nconnection];

  for (int i = 0; i < nconnection; i++) {
    sockfd[i] = -1;
  }

  for (int i = 0; i < nconnection; i++) {
    int fd = socket_connect(hosts[i]);
    if (fd < 0) {
      goto bail;
    }

    sockfd[i] = fd;
  }

  return kite_handle_assign_socket(client, sockfd, nconnection, errmsg, errlen);

bail:
  for (int i = 0; i < nconnection; i++) {
    if (sockfd[i] >= 0) {
      close(sockfd[i]);
    }
  }
  return 1;
}

static xrg_iter_t *get_next_iter(kite_handle_t *client) {
  xrg_iter_t *iter = 0;
  if (client->curr_res) {
    // iterate curr_res
    iter = kite_result_next(client->curr_res->arg);
  }

  if (!iter) {
    if (client->curr_res) {
      listcell_free(client->curr_res);
      client->curr_res = 0;
    }

    listcell_t *lc = list_pop(client->rlist);
    if (lc) {
      client->curr_res = lc;
      iter = kite_result_next(lc->arg);
    }
  }

  return iter;
}

/*
 * return -1 error, 0 success, 1 no more pending data
 */
int kite_next_row(kite_handle_t *client, xrg_iter_t **retiter, char *errmsg,
                  int errlen) {

  int e = 0;
  xrg_iter_t *iter = 0;

  iter = get_next_iter(client);

  if (iter) {
    *retiter = iter;
    return 0;
  }

  while (true) {
    e = event_base_loop(client->evbase, EVLOOP_ONCE);

    if (e == 0) {
      // check e = ? when even_base_loopback called.  probably e = 0
      if (event_base_got_break(client->evbase)) {
        // break because of timed out
        snprintf(errmsg, errlen, "socket timed out");
        return -1;
      }

      // some rows to read
      iter = get_next_iter(client);
      if (!iter) {
        continue;
      }

      *retiter = iter;
      return 0;
    } else if (e == 1) {
      // no more pending events
      *retiter = 0;
      return 0;
    } else {
      // unhandled error
      snprintf(errmsg, errlen, "kite_result_next_row: unhandled error");
      return -1;
    }
  };

  return 0;
}

static void setup_fragment(xstringbuffer_t *sbuf, int fragid, int fragcnt) {

  xstringbuffer_append_string(sbuf, "\"fragment\": ");
  xstringbuffer_append(sbuf, '[');
  xstringbuffer_append_int(sbuf, fragid);
  xstringbuffer_append(sbuf, ',');
  xstringbuffer_append_int(sbuf, fragcnt);
  xstringbuffer_append(sbuf, ']');
}

static void setup_sql(xstringbuffer_t *sbuf, const char *sql) {
  xstringbuffer_append_string(sbuf, "\"sql\": ");
  xstringbuffer_escape_json(sbuf, sql);
}

static void setup_filespec(xstringbuffer_t *sbuf, kite_filespec_t *fs) {
  char value[2];
  memset(value, 0, sizeof(value));

  xstringbuffer_append_string(sbuf, "\"filespec\": ");
  xstringbuffer_append(sbuf, '{');
  xstringbuffer_append_string(sbuf, "\"fmt\": ");
  xstringbuffer_escape_json(sbuf, fs->fmt);

  if (strcasecmp(fs->fmt, "csv") == 0) {
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append_string(sbuf, "\"csvspec\": ");
    xstringbuffer_append(sbuf, '{');
    xstringbuffer_append_string(sbuf, "\"delim\": ");
    value[0] = fs->u.csv.delim;
    xstringbuffer_escape_json(sbuf, value);
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append_string(sbuf, "\"quote\": ");
    value[0] = fs->u.csv.quote;
    xstringbuffer_escape_json(sbuf, value);
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append_string(sbuf, "\"escape\": ");
    value[0] = fs->u.csv.escape;
    xstringbuffer_escape_json(sbuf, value);
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append_string(sbuf, "\"header_line\": ");
    if (fs->u.csv.header_line == 0) {
      xstringbuffer_append_string(sbuf, "false");
    } else {
      xstringbuffer_append_string(sbuf, "true");
    }
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append_string(sbuf, "\"nullstr\": ");
    xstringbuffer_escape_json(sbuf, fs->u.csv.nullstr);

    xstringbuffer_append(sbuf, '}');
  }
  xstringbuffer_append(sbuf, '}');
}

static int is_valid_type(char *type) {
  char *valid_types[] = {"int8", "int16", "int32", "int64", "float", "double", "decimal", "string", "date", "time", "timestamp", "interval"};

  for (int i = 0 ; i < 12 ; i++) {
    if (strcmp(type, valid_types[i]) == 0) {
      return 1;
    }
  }
  return 0;
}

static int setup_schema(xstringbuffer_t *sbuf, char *schema, char *errmsg,
                        int errlen) {

  const char colon = ':';
  const char *sep = "\r\n";
  char *tok;
  char *rest = schema;
  char *cname, *type, *precision, *scale;
  int i = 0;

  xstringbuffer_append_string(sbuf, "\"schema\": [");

  tok = strtok_r(rest, sep, &rest);
  while (tok) {
    char *p1 = strchr(tok, colon);
    if (!p1) {
      // error here
      snprintf(errmsg, errlen, "schema format error");
      return 1;
    }

    *p1 = 0;
    cname = tok;

    char *p2 = strchr(p1 + 1, colon);

    if (p2) {
      char *p3 = strchr(p2 + 1, colon);
      if (!p3) {
        snprintf(errmsg, errlen, "schema format error: precision:scale");
        return 1;
      }
      *p2 = *p3 = 0;
      type = p1 + 1;
      precision = p2 + 1;
      scale = p3 + 1;
    } else {
      type = p1 + 1;
      precision = 0;
      scale = 0;
    }

    if (! is_valid_type(type)) {
        snprintf(errmsg, errlen, "invalid type error");
        return 1;
    }
        
    if (i > 0) {
      xstringbuffer_append(sbuf, ',');
    }
    xstringbuffer_append(sbuf, '{');
    xstringbuffer_append(sbuf, '"');
    xstringbuffer_append_string(sbuf, "name");
    xstringbuffer_append(sbuf, '"');
    xstringbuffer_append(sbuf, ':');
    xstringbuffer_escape_json(sbuf, cname);
    xstringbuffer_append(sbuf, ',');
    xstringbuffer_append(sbuf, '"');
    xstringbuffer_append_string(sbuf, "type");
    xstringbuffer_append(sbuf, '"');
    xstringbuffer_append(sbuf, ':');
    xstringbuffer_escape_json(sbuf, type);

    if (precision) {
      xstringbuffer_append(sbuf, ',');
      xstringbuffer_append(sbuf, '"');
      xstringbuffer_append_string(sbuf, "precision");
      xstringbuffer_append(sbuf, '"');
      xstringbuffer_append(sbuf, ':');
      xstringbuffer_append_string(sbuf, precision);
      xstringbuffer_append(sbuf, ',');
      xstringbuffer_append(sbuf, '"');
      xstringbuffer_append_string(sbuf, "scale");
      xstringbuffer_append(sbuf, '"');
      xstringbuffer_append(sbuf, ':');
      xstringbuffer_append_string(sbuf, scale);
    }
    xstringbuffer_append(sbuf, '}');
    tok = strtok_r(NULL, sep, &rest);
    i++;
  }

  xstringbuffer_append(sbuf, ']');

  return 0;
}

static int setup_schema_json(char **ret, char *schema, char *errmsg,
                             int errlen) {
  int e = 0;
  xstringbuffer_t *sbuf = xstringbuffer_new();
  e = setup_schema(sbuf, schema, errmsg, errlen);
  if (e) {
    goto bail;
  }
  *ret = xstringbuffer_to_string(sbuf);

bail:
  xstringbuffer_release(sbuf);
  return e;
}

static int setup_address(char *addr, char ***addrs, int *naddr, char *errmsg,
                         int errlen) {
  char *tok;
  const char *sep = "\r\n";
  char *rest = addr;
  int maxaddr = 128;
  *addrs = (char **)calloc(maxaddr, sizeof(char *));
  if (!addrs) {
    snprintf(errmsg, errlen, "setup_address, out of memory");
    return 1;
  }
  *naddr = 0;

  tok = strtok_r(rest, sep, &rest);
  while (tok && *naddr < maxaddr) {
    (*addrs)[(*naddr)++] = strdup(tok);

    tok = strtok_r(NULL, sep, &rest);
  }
  return 0;
}

static char *setup_json(const char *schema_json, const char *sql, int fragid,
                        int fragcnt, kite_filespec_t *fs) {
  char *ret = 0;
  xstringbuffer_t *sbuf = xstringbuffer_new();
  xstringbuffer_append(sbuf, '{');
  setup_sql(sbuf, sql);
  xstringbuffer_append(sbuf, ',');
  xstringbuffer_append_string(sbuf, schema_json);
  xstringbuffer_append(sbuf, ',');
  setup_fragment(sbuf, fragid, fragcnt);
  xstringbuffer_append(sbuf, ',');
  setup_filespec(sbuf, fs);
  xstringbuffer_append(sbuf, '}');
  ret = xstringbuffer_to_string(sbuf);
  xstringbuffer_release(sbuf);

  return ret;
}

kite_handle_t *kite_submit(char *addr, const char *schema, const char *sql,
                           int fragid, int fragcnt, kite_filespec_t *fs,
                           char *errmsg, int errlen) {

  int nhost = 0;
  int naddr = 0;
  char **addrs = 0;
  char *schema_json = 0;
  int e = 0;
  char **hosts = 0;
  char **jsons = 0;
  char *schema_cpy = strdup(schema);
  kite_handle_t *hdl = 0;

  e = setup_address(addr, &addrs, &naddr, errmsg, errlen);
  if (e) {
    goto bail;
  }

  e = setup_schema_json(&schema_json, schema_cpy, errmsg, errlen);
  if (e) {
    fprintf(stderr, "setup_schema_sql: error %s", errmsg);
    goto bail;
  }

  if (fragid == -1) {
    nhost = fragcnt;
    hosts = malloc(sizeof(char *) * nhost);
    jsons = malloc(sizeof(char *) * nhost);

    for (int i = 0; i < nhost; i++) {
      int n = i % naddr;
      hosts[i] = addrs[n];
      jsons[i] = setup_json(schema_json, sql, i, fragcnt, fs);
    }

  } else {
    int n = fragid % naddr;
    nhost = 1;
    hosts = malloc(sizeof(char *) * nhost);
    jsons = malloc(sizeof(char *) * nhost);
    hosts[0] = addrs[n];
    jsons[0] = setup_json(schema_json, sql, fragid, fragcnt, fs);
  }

  // connect socket
  hdl = kite_handle_create();
  if (!hdl) {
    snprintf(errmsg, errlen, "kite_handle_create_failed");
    goto bail;
  }

  e = kite_handle_connect(hdl, hosts, nhost, errmsg, errlen);
  if (e) {
    kite_release(hdl);
    hdl = 0;
    goto bail;
  }

  e = kite_handle_exec(hdl, jsons, nhost, errmsg, errlen);
  if (e) {
    kite_release(hdl);
    hdl = 0;
    goto bail;
  }

bail:
  if (addrs) {
    for (int i = 0; i < naddr; i++) {
      if (addrs[i]) {
        free(addrs[i]);
      }
    }
    free(addrs);
  }

  if (schema_json) {
    free(schema_json);
  }

  if (hosts) {
    free(hosts);
  }

  if (jsons) {
    for (int i = 0; i < nhost; i++) {
      if (jsons[i]) {
        free(jsons[i]);
      }
    }
    free(jsons);
  }

  if (schema_cpy) {
    free(schema_cpy);
  }
  return hdl;
}
