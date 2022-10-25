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

#define FRAGMENT "__FRAGMENT__"

static char *replace_fragment(char *json, int fragid, int fragcnt) {
	char *ret = 0;
	char fragment[1024];
	char *p = 0;

	sprintf(fragment, "\"fragment\" : [%d, %d]", fragid, fragcnt);

	char *p1 = strstr(json, FRAGMENT);
	if (! p1) return strdup(json);

	ret = malloc(strlen(json) + strlen(fragment) + 1);
	char *p2 = p1 + strlen(FRAGMENT);

	p = ret;
	strncpy(p, json, p1-json);

	p += p1-json;
	strcpy(p, fragment);
	p += strlen(fragment);
	strcpy(p, p2);

	return ret;
}


int main(int argc, const char *argv[]) {
	char *host;
	const char *jsonfn;
	int e;
	char *json = 0, *newjson;
	int nrow = 0;
	int nconn = 1;

	if (argc != 3) {
		fprintf(stderr, "usage: xcli host:port jsonfn\n");
		return 1;
	}

	host = strdup(argv[1]);
	jsonfn = argv[2];

	json = read_file_fully(jsonfn);

	if (! json) {
		fprintf(stderr, "read file failed\n");
		return 1;
	}

	newjson = replace_fragment(json, 0, 1);
	fprintf(stderr, "json = *%s*", newjson);

	kite_client_t *cli = kite_client_create();

	kite_client_connect(cli, host, nconn);

	kite_client_exec(cli, (const char **) &newjson, nconn);

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
	free(newjson);
	free(host);
	kite_client_destroy(cli);

	return 0;
}
