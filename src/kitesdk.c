#include <unistd.h>
#include <stdlib.h>
#include <event2/event.h>
#include "sockstream.h"
#include "kite_client.h"
#include "kitesdk.h"

typedef struct listcell_t listcell_t;
struct listcell_t {
	kite_result_t *arg;
	listcell_t *next;
};

static void listcell_free(listcell_t *lc) {
	if (lc->arg) kite_result_destroy(lc->arg);
	free(lc);
}

typedef struct list_t list_t;
struct list_t {
	listcell_t *head;
	listcell_t *tail;
};

static bool list_empty(list_t *list) { return (list->head == NULL); }

static list_t *list_append(list_t *list, kite_result_t *arg) {

	list_t *ret = 0;
	listcell_t *lc;

	if (list == NULL) {
		ret = (list_t *) malloc(sizeof(list_t));
		ret->head = ret->tail = 0;
	} else {
		ret = list;
	}

	lc = (listcell_t *) malloc(sizeof(listcell_t));
	lc->arg = arg;
	lc->next = 0;

	if (ret->tail) {
		ret->tail->next = lc;
		ret->tail = lc;	
	} else {
		ret->tail = lc;
	}

	if (ret->head == NULL)  {
		ret->head = ret->tail;
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
		listcell_t* ret = list->head;
		if (list->head) {
			list->head = list->head->next;
			if (list->head == NULL) {
				list->tail = NULL;
			}
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

//typedef struct kite_client_t kite_client_t;
struct kite_client_t {
	struct event_base *evbase;
	kite_evcxt_t *evcxt;
	int nevt;
	list_t *rlist;
	listcell_t *curr_res;

};

kite_client_t *kite_client_create() {
	kite_client_t *client = (kite_client_t *) malloc(sizeof(kite_client_t));
	memset(client, 0, sizeof(kite_client_t));
	client->evbase = event_base_new();
	return client;
}

void kite_client_destroy(kite_client_t *client) {
	if (client) {

		if (client->evcxt) {
			for (int i = 0 ; i < client->nevt ; i++) {
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

static void kite_evcb(evutil_socket_t fd, short what, void *arg)
{
	kite_evcxt_t *cxt = (kite_evcxt_t *) arg;
	kite_client_t *client = (kite_client_t *) cxt->arg;
	if (what & EV_TIMEOUT) {
		event_base_loopbreak(((kite_client_t *) cxt->arg)->evbase);
	} else if (what & EV_READ) {
		kite_result_t *res = kite_get_result(cxt->ss);
		if (! res) {
			event_del(cxt->ev);
			cxt->ev = 0;
			return;
		}

		client->rlist = list_append(client->rlist, res);
	} 
}

void kite_client_exec(kite_client_t *client, const char *json, bool auto_fragment) {

	for (int i = 0 ; i < client->nevt ; i++) {
		if (auto_fragment) {
			// TODO: add fragid and fragcnt  to the json
		}

		kite_exec(client->evcxt[i].ss, json);
	}
}

int kite_client_assign_socket(kite_client_t *client, int *sockfd, int nsocket) {
	struct event *ev;
	struct timeval fivesec = {5,0};

	client->nevt = nsocket;
	client->evcxt = (kite_evcxt_t *) malloc(sizeof(kite_evcxt_t) * nsocket);
	if (!client->evcxt) {
		fprintf(stderr, "kite_client_assign_socket: out of memory\n");
		return 1;
	}

	for (int i = 0 ; i < client->nevt ; i++) {
		client->evcxt[i].ss = sockstream_assign(sockfd[i]);
		client->evcxt[i].arg = client;
		ev = event_new(client->evbase, sockfd[i], EV_TIMEOUT|EV_READ|EV_PERSIST, kite_evcb, &client->evcxt[i]);
		client->evcxt[i].ev = ev;
		event_add(ev, &fivesec);
	}
	return 0;
}


int kite_client_connect(kite_client_t *client, char *host, int nconnection) {

	int sockfd[nconnection];

	for (int i = 0 ; i < nconnection ; i++) {
		sockfd[i] = -1;
	}

	for (int i = 0 ; i < nconnection ; i++) {
		int fd = socket_connect(host);
		if (fd < 0) {
			goto bail;
		}

		sockfd[i] = fd;
	}

	return kite_client_assign_socket(client, sockfd, nconnection);

bail:
	for (int i = 0 ; i < nconnection ; i++) {
		if (sockfd[i] >= 0) {
			close(sockfd[i]);
		}
	}
	return 1;
}


static xrg_iter_t *get_next_iter(kite_client_t *client) {
	xrg_iter_t *iter = 0;
	if (client->curr_res) {
		// iterate curr_res
		iter = kite_result_next(client->curr_res->arg);
	}

	if (! iter) {
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
int kite_client_next_row(kite_client_t *client, xrg_attr_t **attrs, void ***values, char ***flags, int *ncol) {

	int e = 0;
	xrg_iter_t *iter = 0;

	iter = get_next_iter(client);

	if (iter) {
		// return row
		*attrs = iter->attr;
		*values = iter->value;
		*flags = iter->flag;
		*ncol = iter->nvec;

		return 0;
	}

	e = event_base_loop(client->evbase, EVLOOP_ONCE);

	if (e == 0) {
		// check e = ? when even_base_loopback called.  probably e = 0
		if (event_base_got_break(client->evbase)) {
			// break because of timed out
			return -1;
		}

		// some rows to read
		iter = get_next_iter(client);
		if (!iter) {
			return 1;
		}

		*attrs = iter->attr;
		*values = iter->value;
		*flags = iter->flag;
		*ncol = iter->nvec;
		return 0;
	} else if (e == 1) {
		// no more pending events
		return 1;
	} else {
		// unhandled error

		return -1;
	}

	return 0;
}
