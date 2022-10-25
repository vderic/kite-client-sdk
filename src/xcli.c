#ifdef __STDC_ALLOC_LIB__
#define __STDC_WANT_LIB_EXT2__ 1
#else
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kitesdk.h"

int main(int argc, const char *argv[]) {
	char *host;
	const char *jsonfn;
	int e, n;
	FILE *fp;
	char json[2048];
	int nrow = 0;

	if (argc != 3) {
		fprintf(stderr, "usage: xcli host:port jsonfn\n");
		return 1;
	}

	host = strdup(argv[1]);
	jsonfn = argv[2];

	if ((fp = fopen(jsonfn, "r")) == NULL)  {
		fprintf(stderr, "file read error");
		return 2;
	}

	n = fread(json, 1, sizeof(json), fp);
	fclose(fp);

	fprintf(stderr, "json = *%s*", json);

	json[n] = 0;

	kite_client_t *cli = kite_client_create();

	kite_client_connect(cli, host, 1);

	kite_client_exec(cli, json, false);

	e = 0;
	while (e == 0) {
		xrg_attr_t *attrs = 0;
		void **values = 0;
		char **flags = 0;
		int ncol = 0;
		e = kite_client_next_row(cli, &attrs, &values, &flags, &ncol);
		if (e == 0) {
			// data here
	
			nrow++;
		} else if (e == 1) {
			// no more row
			
			fprintf(stderr, "eof nrow = %d\n", nrow);
		} else {
			// error handling
			fprintf(stderr, "error nrow = %d\n", nrow);
		}
	}

	free(host);
	kite_client_destroy(cli);

	return 0;
}
