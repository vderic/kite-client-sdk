#ifndef _KITESDK_H_
#define _KITESDK_H_

typedef struct kite_client_t kite_client_t;

kite_client_t *kite_client_create(int nconnection);

int kite_client_assign_socket(kite_client_t *client, int *sockfd, int nsocket);

int kite_client_connect(kite_client_t *client, char *host);

void kite_client_exec(kite_client_t *client, const char *json);

int kite_client_next_row(kite_client_t *client, xrg_attr_t **attrs, void ***values, char ***flags, int *ncol);

void kite_client_destroy(kite_client_t *client);

#endif
