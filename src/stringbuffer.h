#ifndef __XSTRINGBUFFER_H__
#define __XSTRINGBUFFER_H__

#include <stdbool.h>
#include <stddef.h>

#ifndef XSTRINGBUFFER_INITIAL_BUFFER_SIZE
#define XSTRINGBUFFER_INITIAL_BUFFER_SIZE 64
#endif

typedef struct XStringBuffer xstringbuffer_t;

struct XStringBuffer;

struct XStringBuffer *xstringbuffer_new(void);
struct XStringBuffer *xstringbuffer_new_with_options(const size_t /* initial_size */, const bool /* allow_resize */);

bool xstringbuffer_is_empty(struct XStringBuffer *);
size_t xstringbuffer_get_initial_size(struct XStringBuffer *);
size_t xstringbuffer_get_content_size(struct XStringBuffer *);
size_t xstringbuffer_get_max_size(struct XStringBuffer *);
bool xstringbuffer_is_allow_resize(struct XStringBuffer *);

bool xstringbuffer_clear(struct XStringBuffer *);
void xstringbuffer_release(struct XStringBuffer *);

bool xstringbuffer_ensure_capacity(struct XStringBuffer *, const size_t /* size */);
bool xstringbuffer_shrink(struct XStringBuffer *);

char *xstringbuffer_to_string(struct XStringBuffer *);

bool xstringbuffer_append(struct XStringBuffer *, char);
bool xstringbuffer_append_string(struct XStringBuffer *, const char *);
bool xstringbuffer_append_string_with_options(struct XStringBuffer *, const char *, const size_t /* offset */, const size_t /* length */);
bool xstringbuffer_append_binary(struct XStringBuffer *, const char *, const size_t /* offset */, const size_t /* length */);
bool xstringbuffer_append_bool(struct XStringBuffer *, bool);
bool xstringbuffer_append_short(struct XStringBuffer *, short);
bool xstringbuffer_append_int(struct XStringBuffer *, int);
bool xstringbuffer_append_long(struct XStringBuffer *, long);
bool xstringbuffer_append_long_long(struct XStringBuffer *, long long);
bool xstringbuffer_append_unsigned_short(struct XStringBuffer *, unsigned short);
bool xstringbuffer_append_unsigned_int(struct XStringBuffer *, unsigned int);
bool xstringbuffer_append_unsigned_long(struct XStringBuffer *, unsigned long);
bool xstringbuffer_append_unsigned_long_long(struct XStringBuffer *, unsigned long long);

void xstringbuffer_escape_json(struct XStringBuffer *buffer, const char *str);

#endif
