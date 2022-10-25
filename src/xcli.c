#ifdef __STDC_ALLOC_LIB__
#define __STDC_WANT_LIB_EXT2__ 1
#else
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kitesdk.h"

static char *read_file_fully(const char *fn) {

	int n;
	FILE *f = fopen(fn, "rb");
	if (f == 0) {
		return 0;
	}
	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

	char *string = malloc(fsize + 1);
	if ((n = fread(string, fsize, 1, f)) < 0) {
		return 0;
	}

	fclose(f);

	string[fsize] = 0;
	return string;
}

static char *replace_fragment(char *json, int fragid, int fragcnt) {
	char *ret = 0;
	char fragment[1024];
	char *p = 0;

	sprintf(fragment, ",\"fragment\" : [%d, %d]", fragid, fragcnt);

	char *p1 = strrchr(json, '}');
	if (! p1) return strdup(json);

	ret = malloc(strlen(json) + strlen(fragment) + 1);

	p = ret;
	strncpy(p, json, p1-json);
	p += p1-json;
	strcpy(p, fragment);
	p += strlen(fragment);
	strcpy(p, p1);

	return ret;
}


int main(int argc, const char *argv[]) {
	char *host;
	const char *jsonfn;
	int e;
	char *json = 0;
	char **jsarr = 0;
	int nrow = 0;
	int nconn = 0;

	if (argc != 4) {
		fprintf(stderr, "usage: xcli host:port jsonfn #connection\n");
		return 1;
	}

	host = strdup(argv[1]);
	jsonfn = argv[2];
	nconn = atoi(argv[3]);

	json = read_file_fully(jsonfn);

	if (! json) {
		fprintf(stderr, "read file failed\n");
		return 1;
	}

	jsarr = malloc(sizeof(char *) * nconn);

	for (int i = 0 ; i < nconn ; i++) {
		jsarr[i] = replace_fragment(json, i, nconn);
		fprintf(stderr, "json[%d] = *%s*\n", i, jsarr[i]);
	}

	kite_client_t *cli = kite_client_create();

	kite_client_connect(cli, host, nconn);

	kite_client_exec(cli, (const char **)jsarr, nconn);

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
			fprintf(stderr, "error %d nrow = %d\n", e, nrow);
		}
	}

	free(json);
	for (int i = 0 ; i < nconn ; i++) {
		free(jsarr[i]);
	}
	free(jsarr);
	free(host);
	kite_client_destroy(cli);

	return 0;
}
