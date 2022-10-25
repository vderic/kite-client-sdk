#ifndef _KITESDK_H_
#define _KITESDK_H_

typedef struct kite_client_t kite_client_t;

/* create the kite_client */
kite_client_t *kite_client_create();

/* assign the socket fds to client */
int kite_client_assign_socket(kite_client_t *client, int *sockfd, int nsocket);

/* create the number of sockets (nconnection) to the same host */
int kite_client_connect(kite_client_t *client, char *host, int nconnection);

/* send the json to server. JSON contains SQL and schema ONLY and fragment info will be added inside the client */
void kite_client_exec(kite_client_t *client, const char *json);

/* get the next row from socket */
int kite_client_next_row(kite_client_t *client, xrg_attr_t **attrs, void ***values, char ***flags, int *ncol);

/* destroy the client */
void kite_client_destroy(kite_client_t *client);

#endif
