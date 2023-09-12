#include "stringbuffer.h"
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define XSTRINGBUFFER_WORK_BUFFER_SIZE 21

struct XStringBuffer {
  size_t initial_size;
  size_t content_size;
  size_t max_size;
  char *value;
  bool allow_resize;
  char *work_buffer;
};

// private functions definitions
static bool _xstringbuffer_clear(struct XStringBuffer *);
static bool _xstringbuffer_set_capacity(struct XStringBuffer *, const size_t);
static bool _xstringbuffer_add_numeric_type(struct XStringBuffer *,
                                            const char *, ...);

struct XStringBuffer *xstringbuffer_new() {
  return (
      xstringbuffer_new_with_options(XSTRINGBUFFER_INITIAL_BUFFER_SIZE, true));
}

struct XStringBuffer *xstringbuffer_new_with_options(const size_t initial_size,
                                                     const bool allow_resize) {
  size_t size = 1;

  if (initial_size > 0) {
    size = initial_size;
  }

  struct XStringBuffer *buffer = malloc(sizeof(struct XStringBuffer));

  if (buffer == NULL) {
    return (NULL);
  }

  buffer->initial_size = size;
  buffer->content_size = 0;
  buffer->work_buffer = malloc(XSTRINGBUFFER_WORK_BUFFER_SIZE * sizeof(char));

  buffer->value = NULL;
  if (!_xstringbuffer_clear(buffer)) {
    xstringbuffer_release(buffer);
    return (NULL);
  }

  // set lock/unlock to the content size
  buffer->allow_resize = allow_resize;

  return (buffer);
}

bool xstringbuffer_is_empty(struct XStringBuffer *buffer) {
  return (buffer->content_size == 0);
}

size_t xstringbuffer_get_initial_size(struct XStringBuffer *buffer) {
  return (buffer->initial_size);
}

size_t xstringbuffer_get_content_size(struct XStringBuffer *buffer) {
  return (buffer->content_size);
}

size_t xstringbuffer_get_max_size(struct XStringBuffer *buffer) {
  return (buffer->max_size);
}

bool xstringbuffer_is_allow_resize(struct XStringBuffer *buffer) {
  return (buffer->allow_resize);
}

bool xstringbuffer_clear(struct XStringBuffer *buffer) {
  if (buffer == NULL) {
    return (false);
  }

  // already empty
  if (buffer->content_size == 0) {
    return (true);
  }

  return (_xstringbuffer_clear(buffer));
}

bool xstringbuffer_ensure_capacity(struct XStringBuffer *buffer,
                                   const size_t size) {
  if (buffer == NULL) {
    return (false);
  }

  if (size <= buffer->max_size) {
    return (true);
  }

  return (_xstringbuffer_set_capacity(buffer, size));
}

bool xstringbuffer_shrink(struct XStringBuffer *buffer) {
  if (buffer == NULL) {
    return (false);
  }

  if (buffer->content_size == buffer->max_size) {
    return (true);
  }

  return (_xstringbuffer_set_capacity(buffer, buffer->content_size));
}

void xstringbuffer_release(struct XStringBuffer *buffer) {
  if (buffer == NULL) {
    return;
  }

  if (buffer->value != NULL) {
    free(buffer->value);
    buffer->value = NULL;
  }

  if (buffer->work_buffer != NULL) {
    free(buffer->work_buffer);
    buffer->work_buffer = NULL;
  }

  free(buffer);
}

bool xstringbuffer_append(struct XStringBuffer *buffer, char character) {
  if (buffer == NULL) {
    return (false);
  }

  if (buffer->content_size == buffer->max_size) {
    const size_t new_size = buffer->content_size * 2;
    if (!_xstringbuffer_set_capacity(buffer, new_size)) {
      return (false);
    }
  }

  buffer->value[buffer->content_size] = character;
  buffer->content_size++;
  buffer->value[buffer->content_size] = 0;

  return (true);
}

bool xstringbuffer_append_string(struct XStringBuffer *buffer,
                                 const char *string) {
  if (string == NULL) {
    return (true);
  }

  const size_t length = strlen(string);

  return (xstringbuffer_append_string_with_options(buffer, string, 0, length));
}

bool xstringbuffer_append_string_with_options(struct XStringBuffer *buffer,
                                              const char *string,
                                              const size_t offset,
                                              const size_t length) {
  if (buffer == NULL) {
    return (false);
  }

  if (string == NULL) {
    return (true);
  }

  const size_t string_length = strlen(string);
  if (offset >= string_length) {
    return (false);
  }

  if ((string_length - offset) < length) {
    return (false);
  }

  return (xstringbuffer_append_binary(buffer, string, offset, length));
} /* xstringbuffer_append_string_with_options */

bool xstringbuffer_append_binary(struct XStringBuffer *buffer,
                                 const char *content, const size_t offset,
                                 const size_t length) {
  if (buffer == NULL) {
    return (false);
  }

  if (content == NULL) {
    return (true);
  }

  const size_t loop_end = length + offset;

  const size_t size_left = buffer->max_size - buffer->content_size;
  if (size_left < length) {
    const size_t min_new_size = buffer->content_size + length;
    size_t new_size = buffer->max_size * 2;
    while (new_size < min_new_size) {
      new_size = new_size * 2;
    }

    if (!_xstringbuffer_set_capacity(buffer, new_size)) {
      return (false);
    }
  }

  for (size_t index = offset, content_index = 0; index < loop_end;
       index++, content_index++) {
    buffer->value[buffer->content_size] = content[index];
    buffer->content_size++;
  }

  buffer->value[buffer->content_size] = 0;

  return (true);
}

char *xstringbuffer_to_string(struct XStringBuffer *buffer) {
  if (buffer == NULL || buffer->content_size == 0) {
    char *string_copy = malloc(sizeof(char));
    if (string_copy == NULL) {
      return (NULL);
    }
    string_copy[0] = 0;
    return (string_copy);
  }

  size_t content_size = buffer->content_size;
  size_t memory_size = (content_size + 1) * sizeof(char);
  char *string_copy = malloc(memory_size);
  if (string_copy == NULL) {
    return (NULL);
  }

  buffer->value[content_size] = 0;
  string_copy[content_size] = 0;

  memcpy(string_copy, buffer->value, memory_size);
  string_copy[content_size] = 0;

  return (string_copy);
}

bool xstringbuffer_append_bool(struct XStringBuffer *buffer, bool value) {
  char *string = value ? "true" : "false";

  return (xstringbuffer_append_string(buffer, string));
}

bool xstringbuffer_append_short(struct XStringBuffer *buffer, short value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%hi", value));
}

bool xstringbuffer_append_int(struct XStringBuffer *buffer, int value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%i", value));
}

bool xstringbuffer_append_long(struct XStringBuffer *buffer, long value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%li", value));
}

bool xstringbuffer_append_long_long(struct XStringBuffer *buffer,
                                    long long value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%lli", value));
}

bool xstringbuffer_append_unsigned_short(struct XStringBuffer *buffer,
                                         unsigned short value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%hu", value));
}

bool xstringbuffer_append_unsigned_int(struct XStringBuffer *buffer,
                                       unsigned int value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%u", value));
}

bool xstringbuffer_append_unsigned_long(struct XStringBuffer *buffer,
                                        unsigned long value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%lu", value));
}

bool xstringbuffer_append_unsigned_long_long(struct XStringBuffer *buffer,
                                             unsigned long long value) {
  return (_xstringbuffer_add_numeric_type(buffer, "%llu", value));
}

static bool _xstringbuffer_clear(struct XStringBuffer *buffer) {
  if (buffer == NULL) {
    return (false);
  }

  if (buffer->value != NULL) {
    free(buffer->value);
    buffer->value = NULL;
  }

  buffer->max_size = buffer->initial_size;
  buffer->content_size = 0;

  buffer->value = malloc((buffer->max_size + 1) * sizeof(char));
  if (buffer->value == NULL) {
    xstringbuffer_release(buffer);
    return (false);
  }

  buffer->value[0] = 0;
  buffer->value[buffer->max_size] = 0;

  return (true);
}

static bool _xstringbuffer_set_capacity(struct XStringBuffer *buffer,
                                        const size_t size) {
  if (!buffer->allow_resize) {
    return (false);
  }

  buffer->max_size = size;
  buffer->value = realloc(buffer->value, (buffer->max_size + 1) * sizeof(char));

  // put null at end
  buffer->value[buffer->content_size] = 0;
  buffer->value[buffer->max_size] = 0;

  return (true);
}

static bool _xstringbuffer_add_numeric_type(struct XStringBuffer *buffer,
                                            const char *format, ...) {
  if (buffer == NULL) {
    return (false);
  }

  va_list args;
  va_start(args, format);
  const int length = vsnprintf(buffer->work_buffer,
                               XSTRINGBUFFER_WORK_BUFFER_SIZE, format, args);
  va_end(args);

  if (length <= 0) {
    return (false);
  }

  const bool result = xstringbuffer_append_string_with_options(
      buffer, buffer->work_buffer, 0, (size_t)length);

  return (result);
}

void xstringbuffer_escape_json(struct XStringBuffer *buffer, const char *str) {
  const char *p;

  xstringbuffer_append(buffer, '\"');
  for (p = str; *p; p++) {
    switch (*p) {
    case '\b':
      xstringbuffer_append_string(buffer, "\\b");
      break;
    case '\f':
      xstringbuffer_append_string(buffer, "\\f");
      break;
    case '\n':
      xstringbuffer_append_string(buffer, "\\n");
      break;
    case '\r':
      xstringbuffer_append_string(buffer, "\\r");
      break;
    case '\t':
      xstringbuffer_append_string(buffer, "\\t");
      break;
    case '"':
      xstringbuffer_append_string(buffer, "\\\"");
      break;
    case '\\':
      xstringbuffer_append_string(buffer, "\\\\");
      break;
    default:
      if ((unsigned char)*p < ' ') {
        char hex[16];
        sprintf(hex, "\\u%04x", (int)*p);
        xstringbuffer_append_string(buffer, hex);
      } else {
        xstringbuffer_append(buffer, *p);
      }
      break;
    }
  }
  xstringbuffer_append(buffer, '\"');
}
