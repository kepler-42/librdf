/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * rdf_hash_tokyodb.c - RDF hash Tokyo DB Interface Implementation
 *
 * Copyright (C) 2015, kepler-42
 *
 * This package is Free Software and part of Redland http://librdf.org/
 *
 * It is licensed under the following three licenses as alternatives:
 *   1. GNU Lesser General Public License (LGPL) V2.1 or any newer version
 *   2. GNU General Public License (GPL) V2 or any newer version
 *   3. Apache License, V2.0 or any newer version
 *
 * You may not use this file except in compliance with at least one of
 * the above three licenses.
 *
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * complete terms and further detail along with the license texts for
 * the licenses in COPYING.LIB, COPYING and LICENSE-2.0.txt respectively.
 *
 *
 */

#ifdef HAVE_CONFIG_H
#include <rdf_config.h>
#endif

#ifdef WIN32
#include <win32_rdf_config.h>
#endif

#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>

/* for the memory allocation functions */
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif

// TODO: Add configure checks
#include <tcutil.h>
#include <tcbdb.h>

#include <redland.h>
#include <rdf_hash.h>


typedef struct
{
  librdf_hash *hash;
  int mode;
  int is_writable;
  int is_new;
  /* for Tokyo DB only */
  TCBDB *db;
  char* file_name;
} librdf_hash_tokyodb_context;

/* internal function */
static int librdf_hash_tokyodb_cursor_find_and_perform_action(const librdf_hash_tokyodb_context *context, BDBCUR *cur, const char *const key, int key_size,
                                const char *const value, int value_size, bool read_only,
                                char **out_last_key, int *out_last_key_size, char **out_last_value, int *out_last_value_size);

/* Implementing the hash cursor */
static int librdf_hash_tokyodb_cursor_init(void *cursor_context, void *hash_context);
static int librdf_hash_tokyodb_cursor_get(void *context, librdf_hash_datum* key, librdf_hash_datum* value, unsigned int flags);
static void librdf_hash_tokyodb_cursor_finish(void* context);

/* prototypes for local functions */
static int librdf_hash_tokyodb_create(librdf_hash* hash, void* context);
static int librdf_hash_tokyodb_destroy(void* context);
static int librdf_hash_tokyodb_open(void* context, const char *identifier, int mode, int is_writable, int is_new, librdf_hash* options);
static int librdf_hash_tokyodb_close(void* context);
static int librdf_hash_tokyodb_clone(librdf_hash* new_hash, void *new_context, char *new_identifier, void* old_context);
static int librdf_hash_tokyodb_values_count(void *context);
static int librdf_hash_tokyodb_put(void* context, librdf_hash_datum *key, librdf_hash_datum *data);
static int librdf_hash_tokyodb_exists(void* context, librdf_hash_datum *key, librdf_hash_datum *value);
static int librdf_hash_tokyodb_delete_key(void* context, librdf_hash_datum *key);
static int librdf_hash_tokyodb_delete_key_value(void* context, librdf_hash_datum *key, librdf_hash_datum *value);
static int librdf_hash_tokyodb_sync(void* context);
static int librdf_hash_tokyodb_get_fd(void* context);

static void librdf_hash_tokyodb_register_factory(librdf_hash_factory *factory);


/* functions implementing hash api */

/**
 * librdf_hash_tokyodb_create:
 * @hash: #librdf_hash hash that this implements
 * @context: Tokyo DB hash context
 *
 * Create a Tokyo DB hash.
 *
 * Return value: non 0 on failure.
 **/
static int
librdf_hash_tokyodb_create(librdf_hash* hash, void* context)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;
  db_context->hash=hash;
  return 0;
}


/**
 * librdf_hash_tokyodb_destroy:
 * @context: Tokyo DB hash context
 *
 * Destroy a Tokyo DB hash.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_destroy(void* context)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;
  /* NOP */
  return 0;
}


/**
 * librdf_hash_tokyodb_open:
 * @context: Tokyo DB hash context
 * @identifier: filename to use for Tokyo DB file
 * @mode: file creation mode
 * @is_writable: is hash writable?
 * @is_new: is hash new?
 * @options: hash options (currently unused)
 *
 * Open and maybe create a Tokyo DB hash.
 *
 * Return value: non 0 on failure.
 **/
static int
librdf_hash_tokyodb_open(void* context, const char *identifier,
                          int mode, int is_writable, int is_new, librdf_hash* options)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  LIBRDF_ASSERT_OBJECT_POINTER_RETURN_VALUE(identifier, cstring, 1);
  char *file = LIBRDF_MALLOC(char*, strlen(identifier) + 4);
  if(!file) {
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: open of '%s' failed - malloc failed", __FUNCTION__, file);
    return -1;
  }
  sprintf(file, "%s.db", identifier);

  /* NOTE: If the options parameter is ever used here, the data must be
   * copied into a private part of the context so that the clone
   * method can access them
   */
  db_context->mode=mode;
  db_context->is_writable=is_writable;
  db_context->is_new=is_new;

  int omode;
  if (is_writable)
    omode = BDBOWRITER;
  else
    omode = BDBOREADER;

  if (is_new)
    omode |= BDBOCREAT;

  db_context->db = tcbdbnew();
  if (db_context->db == NULL) {
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: open of '%s' failed - unable to createobject", __FUNCTION__, file);
    LIBRDF_FREE(char*, file);
    return -1;
  }

  if(!tcbdbopen(db_context->db, file, omode)){
    int ecode = tcbdbecode(db_context->db);
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: open of '%s' failed - %s", __FUNCTION__, file, tcbdberrmsg(ecode));
    tcbdbdel(db_context->db);
    LIBRDF_FREE(char*, file);
    return -1;
  }

  db_context->file_name = file;
  return 0;
}


/**
 * librdf_hash_tokyodb_close:
 * @context: Tokyo DB hash context
 *
 * Close the hash.
 *
 * Finish the association between the rdf hash and the tokyodb file (does
 * not delete the file)
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_close(void* context)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  int ret = 0;
  /* close the database */
  if (db_context->db != NULL) {
    if(!tcbdbclose(db_context->db)){
      int ecode = tcbdbecode(db_context->db);
      librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
          "%s: close failed - %s", __FUNCTION__, tcbdberrmsg(ecode));
      ret = -1;
    }

    tcbdbdel(db_context->db);
    db_context->db = NULL;
  }
  LIBRDF_FREE(char*, db_context->file_name);
  return ret;
}


/**
 * librdf_hash_tokyodb_clone:
 * @hash: new #librdf_hash that this implements
 * @context: new Tokyo DB hash context
 * @new_identifier: new identifier for this hash
 * @old_context: old Tokyo DB hash context
 *
 * Clone the Tokyo DB hash.
 *
 * Clones the existing Tokyo DB hash into the new one with the
 * new identifier.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_clone(librdf_hash *hash, void* context, char *new_identifier,
    void *old_context)
{
  librdf_hash_tokyodb_context* hcontext=(librdf_hash_tokyodb_context*)context;
  librdf_hash_tokyodb_context* old_hcontext=(librdf_hash_tokyodb_context*)old_context;
  librdf_hash_datum *key, *value;
  librdf_iterator *iterator;
  int status=0;

  /* copy data fields that might change */
  hcontext->hash = hash;

  /* Note: The options are not used at present, so no need to make a copy
   */
  if(librdf_hash_tokyodb_open(context, new_identifier,
      old_hcontext->mode, old_hcontext->is_writable,
      old_hcontext->is_new, NULL))
    return -1;

  /* Use higher level functions to iterator this data
   * on the other hand, maybe this is a good idea since that
   * code is tested and works
   */
  key = librdf_new_hash_datum(hash->world, NULL, 0);
  value = librdf_new_hash_datum(hash->world, NULL, 0);

  iterator=librdf_hash_get_all(old_hcontext->hash, key, value);
  while(!librdf_iterator_end(iterator)) {
    librdf_hash_datum* k= (librdf_hash_datum*)librdf_iterator_get_key(iterator);
    librdf_hash_datum* v= (librdf_hash_datum*)librdf_iterator_get_value(iterator);

    if(librdf_hash_tokyodb_put(hcontext, k, v)) {
      status=1;
      break;
    }
    librdf_iterator_next(iterator);
  }
  if(iterator)
    librdf_free_iterator(iterator);

  librdf_free_hash_datum(value);
  librdf_free_hash_datum(key);

  return status;
}


/**
 * librdf_hash_tokyodb_values_count:
 * @context: Tokyo DB hash context
 *
 * Get the number of values in the hash.
 *
 * Return value: number of values in the hash or <0 if not available
 **/
static int
librdf_hash_tokyodb_values_count(void *context)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  return tcbdbrnum(db_context->db);
}


typedef struct {
  librdf_hash_tokyodb_context* hash_context;
  bool cursor_set_to_first;
  char *last_key;
  int last_key_size;
  char *last_value;
  int last_value_size;
  BDBCUR *cur;
} librdf_hash_tokyodb_cursor_context;


/**
 * librdf_hash_tokyodb_cursor_init:
 * @cursor_context: hash cursor context
 * @hash_context: hash to operate over
 *
 * Initialize a new bdb cursor.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_cursor_init(void *cursor_context, void *hash_context)
{
  librdf_hash_tokyodb_cursor_context *cursor=(librdf_hash_tokyodb_cursor_context*)cursor_context;
  cursor->hash_context=(librdf_hash_tokyodb_context*)hash_context;

  cursor->cur = tcbdbcurnew(cursor->hash_context->db);
  if (cursor->cur == NULL) {
    /* error: failed to create cursor */
    int ecode = tcbdbecode(cursor->hash_context->db);
    librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: cursor init failed - unable to create cursor object - %s", __FUNCTION__, tcbdberrmsg(ecode));
    return -1;
  }
  cursor->cursor_set_to_first = false;
  return 0;
}

/**
 * librdf_hash_tokyodb_cursor_find_and_perform_action:
 * @context: Tokyo DB context
 * @cur: Tokyo DB cursor
 * @key: pointer to key to use
 * @key_size: size of the key
 * @value: pointer to value to use
 * @value_size: size of the value
 * @read_only: determines the action to be performed on given key/value. If true, returns the key/value in out parameters,
 *             otherwise deletes the key/value.
 * @out_last_key: [out parameter] pointer to variable where found key will be returned
 * @out_last_key_size: [out parameter] pointer to variable where size of found key will be returned
 * @out_last_value: [out parameter] pointer to variable where found value will be returned
 * @out_last_value_size: [out parameter] pointer to variable where size of found value will be returned
 *
 * Finds and performs the action on given key/value. the actions are determined by read_only parameter.
 *
 * Return value: 0 when key/value is found, 1 when not found, and non 0 on failure
 **/
static int librdf_hash_tokyodb_cursor_find_and_perform_action(const librdf_hash_tokyodb_context *context, BDBCUR *cur, const char *const key, int key_size,
    const char *const value, int value_size,
    bool read_only, char **out_last_key, int *out_last_key_size, char **out_last_value, int *out_last_value_size) {
  if (read_only && (!out_last_key || !out_last_value))
    return -1;

  int ret = 1;
  char *db_key = NULL;
  int db_key_size = 0;

  char *db_value = NULL;
  int db_value_size = 0;

  bool has_data = true;
  while(has_data && ((db_key = tcbdbcurkey(cur, &db_key_size)) != NULL)) {
    /* NOTE: For the first record we don't need to compare keys, but for subsequent calls we need to, hence the comparison  */
    if ((key_size == db_key_size) && !memcmp(key, db_key, key_size)) {
      /* find the record matching key and value */
      if((db_value = tcbdbcurval(cur, &db_value_size)) != NULL) {
        if (value != NULL) {
          if (read_only) {
            if ((value_size != db_value_size) || memcmp(value, db_value, db_value_size))
              ret = 0; /* we found the next record of the given key/value */
          }
          else if ((value_size == db_value_size) && !memcmp(value, db_value, db_value_size))
            ret = 0; /* we found the exact match for given key/value */
          /* else left out intentionally */
        }
        else {
          /* we found the key, no need to search for value */
          ret = 0;
        }
      }
      else {
        /* error: failed to get value, but key was found */
        int ecode = tcbdbecode(context->db);
        librdf_log(context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
            "%s: Failed to get value for key %s - %s", __FUNCTION__, db_key, tcbdberrmsg(ecode));
        ret = -1;
      }
    }

    if (read_only) {
      has_data = tcbdbcurnext(cur); /* irrespective of whether we find key/value or not, move the cursor to next so that next call works */

      if (!ret) {
        /* we found the data, return the key/value in out paraemters */
        if (*out_last_key) /* When iterating, upper layer expects us to free memory for old cursor data */
          LIBRDF_FREE(const char*, *out_last_key);
        *out_last_key = db_key;
        *out_last_key_size = db_key_size;

        if (*out_last_value) /* When iterating, upper layer expects us to free memory for old cursor data */
          LIBRDF_FREE(const char*, *out_last_value);
        *out_last_value = db_value;
        *out_last_value_size = db_value_size;
        has_data = false; /* we found the data, hence exit */
      }
    }
    else {
      if (!ret) {
        /* we found the data, delete the key/value*/
        if (tcbdbcurout(cur)) {
          ret = 0;
        }
        else {
          /* error: failed to delete record */
          int ecode = tcbdbecode(context->db);
          librdf_log(context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
              "%s: Failed to delete: '%s'->'%s' - %s", __FUNCTION__, db_key, db_value, tcbdberrmsg(ecode));
          ret = -1;
        }
      }
      has_data = false; /* for delete case, we exit irrespective of result */
    }

    if (!read_only || ret) {
      SYSTEM_FREE(db_key);
      if (db_value) {
        SYSTEM_FREE(db_value);
        db_value = NULL;
      }
    }
  }

  return ret;
}

/**
 * librdf_hash_tokyodb_cursor_get:
 * @context: Tokyo DB hash cursor context
 * @key: pointer to key to use
 * @value: pointer to value to use
 * @flags: flags
 *
 * Retrieve a hash value for the given key.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_cursor_get(void* context,
    librdf_hash_datum *key, librdf_hash_datum *value,
    unsigned int flags)
{
  librdf_hash_tokyodb_cursor_context *cursor=(librdf_hash_tokyodb_cursor_context*)context;

  /* NOTE: value can be NULL, but not the key */
  if (key == NULL) {
    librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: key is NULL, this is invalid use case !!!", __FUNCTION__);
    return -1;
  }

  switch(flags) {
  case LIBRDF_HASH_CURSOR_SET:
    if (key->data == NULL) {
      librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
          "%s: LIBRDF_HASH_CURSOR_SET and key is NULL, we do not support such use case !!!", __FUNCTION__);
      return -1;
    }
    else if (cursor->last_key != NULL) {
      librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_HASH, NULL,
          "%s: Invalid condition, cursor->last_key should be NULL", __FUNCTION__);
      return -1;
    }
    else if (cursor->last_value != NULL) {
      librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_HASH, NULL,
          "%s: Invalid condition, cursor->last_value should be NULL", __FUNCTION__);
      return -1;
    }
    else if (value != NULL && value->data != NULL) {
      librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
          "%s: LIBRDF_HASH_CURSOR_SET and value->data is NULL, not sure if this use case works !!!", __FUNCTION__);
      return -1;
    }
    else if (!tcbdbcurjump(cursor->cur, key->data, LIBRDF_BAD_CAST(int, key->size))) {
      /* No key found */
      return -1;
    }
    else {
      /* NOP */
    }
    break;

  case LIBRDF_HASH_CURSOR_FIRST:
    if (!tcbdbcurfirst(cursor->cur))
      return -1;

    cursor->cursor_set_to_first = true;
    break;

  case LIBRDF_HASH_CURSOR_NEXT_VALUE:
    break;

  case LIBRDF_HASH_CURSOR_NEXT:
    /* for calls like librdf_hash_get_as_boolean / librdf_hash_get_as_long, LIBRDF_HASH_CURSOR_NEXT can be called even if
     *  LIBRDF_HASH_CURSOR_SET or LIBRDF_HASH_CURSOR_FIRST is not called */
    if (!cursor->cursor_set_to_first) {
      if (!tcbdbcurjump(cursor->cur, key->data, LIBRDF_BAD_CAST(int, key->size))) {
        /* No key found */
        return -1;
      }
    }

    break;

  default:
    librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_HASH, NULL,
        "%s: Unknown hash method flag %d", __FUNCTION__, flags);
    return -1;
  }

  if (flags == LIBRDF_HASH_CURSOR_SET || flags == LIBRDF_HASH_CURSOR_NEXT_VALUE) {
    char *key_to_compare = NULL;
    int key_size = 0;

    if (key->data != NULL) {
      key_to_compare = key->data;
      key_size = LIBRDF_BAD_CAST(int, key->size);
    }
    else {
      key_to_compare = cursor->last_key;
      key_size = cursor->last_key_size;
    }

    char *value_to_compare = value->data;
    int value_size = 0;

    if (value_to_compare)
      value_size = LIBRDF_BAD_CAST(int, value->size);

    int ret = librdf_hash_tokyodb_cursor_find_and_perform_action(cursor->hash_context, cursor->cur, key_to_compare, key_size,
                                    value_to_compare, value_size, true,
                                    &cursor->last_key, &cursor->last_key_size, &cursor->last_value, &cursor->last_value_size);
    if (!ret) {
      value->data = cursor->last_value;
      value->size = LIBRDF_GOOD_CAST(int, cursor->last_value_size);
    }

    return ret;
  }

  int ret = -1;
  bool exit = false;
  char *db_key = NULL;
  char *db_value = NULL;

  /* Must mess about finding next key - note this relies on
   * the tokyodb btree having the keys in sorted order
   */
  int db_key_size = 0;
  while((db_key = tcbdbcurkey(cursor->cur, &db_key_size)) != NULL) {
    /* NOTE: We have already confirmed that key->data is null in non LIBRDF_HASH_CURSOR_SET cases */
    if (value != NULL) {
      /* return keys and values - DON'T check for unique keys */

      int db_value_size = 0;
      db_value = tcbdbcurval(cursor->cur, &db_value_size);
      if(db_value != NULL) {
        if (cursor->last_key) /* When iterating, upper layer expects us to free memory for old cursor data */
          LIBRDF_FREE(const char*, cursor->last_key);

        cursor->last_key = db_key;
        cursor->last_key_size = db_key_size;
        key->data = cursor->last_key;
        key->size = LIBRDF_GOOD_CAST(int, db_key_size);

        if (cursor->last_value) /* When iterating, upper layer expects us to free memory for old cursor data */
          LIBRDF_FREE(const char*, cursor->last_value);

        cursor->last_value = db_value;
        cursor->last_value_size = db_value_size;
        value->data = cursor->last_value;
        value->size = LIBRDF_GOOD_CAST(int, db_value_size);

        ret = 0;
      }
      else {
        /* error: failed to get value, but key was found */
        int ecode = tcbdbecode(cursor->hash_context->db);
        librdf_log(cursor->hash_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
            "%s: Failed to get value for key %s - %s", __FUNCTION__, db_key, tcbdberrmsg(ecode));
      }
      exit = true; /* we have got data, now exit */
    }
    else if (cursor->last_key == NULL) {
      /* return ONLY unique keys */
      /* This is the first key so just return as it is */
      if (cursor->last_key) /* When iterating, upper layer expects us to free memory for old cursor data */
        LIBRDF_FREE(const char*, cursor->last_key);

      cursor->last_key = db_key;
      cursor->last_key_size = db_key_size;
      key->data = cursor->last_key;
      key->size = LIBRDF_GOOD_CAST(int, db_key_size);

      ret = 0;
    }
    else if(memcmp(cursor->last_key, db_key, db_key_size) != 0) {
      /* return ONLY unique keys */
      /* value is NULL and this is not the first call (last_key is not null)
       * which means that we need next unique key
       * keep going until the key changes */
      if (cursor->last_key) /* When iterating, upper layer expects us to free memory for old cursor data */
        LIBRDF_FREE(const char*, cursor->last_key);

      cursor->last_key = db_key;
      cursor->last_key_size = db_key_size;
      key->data = cursor->last_key;
      key->size = LIBRDF_GOOD_CAST(int, db_key_size);
      ret = 0;
    }
    else {
      /* free the key since it is duplicate one and we will still continue our search in loop */
      SYSTEM_FREE(db_key);
      db_key = NULL;
    }

    tcbdbcurnext(cursor->cur);

    if (exit || (ret == 0))
      break;
  }

  return ret;
}


/**
 * librdf_hash_tokyodb_cursor_finished:
 * @context: Tokyo DB hash cursor context
 *
 * Finish the serialization of the hash bdb get.
 *
 **/
static void
librdf_hash_tokyodb_cursor_finish(void* context)
{
  librdf_hash_tokyodb_cursor_context* cursor=(librdf_hash_tokyodb_cursor_context*)context;

  if (cursor->cur) {
    tcbdbcurdel(cursor->cur);
    cursor->cur = NULL;
  }

  if(cursor->last_key)
    LIBRDF_FREE(char*, cursor->last_key);

  if(cursor->last_value)
    LIBRDF_FREE(char*, cursor->last_value);
}


/**
 * librdf_hash_tokyodb_put:
 * @context: Tokyo DB hash context
 * @key: pointer to key to store
 * @value: pointer to value to store
 *
 * Store a key/value pair in the hash.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_put(void* context, librdf_hash_datum *key,
    librdf_hash_datum *value)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  /* NOTE:
   * 1) Sine we want to allow duplicate keys, we use tcbdbputdup
   * 2) Don't use tcbdbputdup2, as we should specify the length to tokyodb otherwise it
   * will take NULL terminated string which creates problem */
  if(!tcbdbputdup(db_context->db, (char*)key->data, LIBRDF_BAD_CAST(int, key->size), (char*)value->data, LIBRDF_BAD_CAST(int, value->size))){
    /* error: failed to put key/value */
    int ecode = tcbdbecode(db_context->db);
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: put failed for %s -> %s - %s", __FUNCTION__, (char*)key->data, (char*)value->data, tcbdberrmsg(ecode));
    return -1;
  }

  return 0;
}


/**
 * librdf_hash_tokyodb_exists:
 * @context: Tokyo DB hash context
 * @key: pointer to key
 * @value: pointer to value (optional)
 *
 * Test the existence of a key/value in the hash.
 *
 * The value can be NULL in which case the check will just be
 * for the key.
 *
 * Return value: >0 if the key/value exists in the hash, 0 if not, <0 on failure
 **/
static int
librdf_hash_tokyodb_exists(void* context, librdf_hash_datum *key,
    librdf_hash_datum *value)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  /* Initialize Tokyo DB key */
  char *temp_key= (char*)key->data;
  int temp_key_size = LIBRDF_BAD_CAST(int, key->size);

  int ret = 0; // not found
  TCLIST *list = tcbdbget4(db_context->db, temp_key, temp_key_size);
  if (list != NULL) {
    if (value == NULL) {
      ret = 1; /* key found - No need to search for value as it is not provided */
    }
    else {
      /* Initialize Tokyo DB value */
      char *temp_value = (char*)value->data;
      int temp_value_size = LIBRDF_BAD_CAST(int, value->size);

      int rnum = tclistnum(list);
      int i;
      for(i = 0; i < rnum ; i++){
        int list_value_size = 0;
        const char *list_value = tclistval(list, i, &list_value_size);
        if((temp_value_size == list_value_size) && !memcmp(temp_value, list_value, list_value_size)) {
          ret = 1; /* key and value found */
          break;
        }
      }
    }

    tclistdel(list);
  }
  return ret;
}

/**
 * librdf_hash_tokyodb_delete_key:
 * @context: Tokyo DB hash context
 * @key: key
 *
 * Delete all values for given key from the hash.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_delete_key(void* context, librdf_hash_datum *key)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  int ret = librdf_hash_tokyodb_exists(context, key, NULL);

  /* NOTE: If there are duplicates all of them will be removed */
  if (!tcbdbout3(db_context->db, key->data, LIBRDF_BAD_CAST(int, key->size))) {
    /* error: failed to get value, but key was found */
    int ecode = tcbdbecode(db_context->db);
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
        "%s: delete failed for key: %s - %s", __FUNCTION__, (char*)key->data, tcbdberrmsg(ecode));
    return -1;
  }
  return 0;
}


/**
 * librdf_hash_tokyodb_delete_key_value:
 * @context: Tokyo DB hash context
 * @key: key
 * @value: value
 *
 * Delete given key/value from the hash.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_delete_key_value(void* context, librdf_hash_datum *key, librdf_hash_datum *value)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;

  int rets = librdf_hash_tokyodb_exists(context, key, value);

  /* NOTE: In tokyo db the only way to delete key/value pair is to use cursor */
  BDBCUR *cur = tcbdbcurnew(db_context->db);
  if (cur == NULL) {
    /* error: failed to create cursor */
    int ecode = tcbdbecode(db_context->db);
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
          "%s: delete failed for: '%s'->'%s' - unable to create cursor object - %s", __FUNCTION__,
          (char *)key->data, (char *)value->data, tcbdberrmsg(ecode));
    return -1;
  }

  int ret = -1;
  if (tcbdbcurjump(cur, key->data, key->size)) {
    ret = librdf_hash_tokyodb_cursor_find_and_perform_action(db_context, cur, (char *)key->data, LIBRDF_BAD_CAST(int, key->size),
                                (char *)value->data, LIBRDF_BAD_CAST(int, value->size), false, NULL, NULL, NULL, NULL);
  }

  tcbdbcurdel(cur);
  return ret;
}


/**
 * librdf_hash_tokyodb_sync:
 * @context: Tokyo DB hash context
 *
 * Flush the hash to disk.
 *
 * Return value: non 0 on failure
 **/
static int
librdf_hash_tokyodb_sync(void* context)
{
  librdf_hash_tokyodb_context* db_context=(librdf_hash_tokyodb_context*)context;
  librdf_log(db_context->hash->world, 0, LIBRDF_LOG_DEBUG, LIBRDF_FROM_STORAGE, NULL,
      "%s: started", __FUNCTION__);

  if (tcbdbsync(db_context->db))
    return 0;
  else {
    /* error: failed to sync */
    int ecode = tcbdbecode(db_context->db);
    librdf_log(db_context->hash->world, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
          "sync failed - %s", tcbdberrmsg(ecode));
  }

  return -1;
}


/**
 * librdf_hash_tokyodb_get_fd:
 * @context: Tokyo DB hash context
 *
 * Get the file description representing the hash.
 *
 * Return value: the file descriptor or < 0 on failure
 **/
static int
librdf_hash_tokyodb_get_fd(void* context)
{
  /* NOP */
  return -1;
}


/* local function to register BDB hash functions */

/**
 * librdf_hash_tokyodb_register_factory:
 * @factory: hash factory prototype
 *
 * Register the Tokyo DB hash module with the hash factory.
 *
 **/
static void
librdf_hash_tokyodb_register_factory(librdf_hash_factory *factory)
{
//  printf ("%s: started\n", __FUNCTION__);
  factory->context_length = sizeof(librdf_hash_tokyodb_context);
  factory->cursor_context_length = sizeof(librdf_hash_tokyodb_cursor_context);

  factory->create  = librdf_hash_tokyodb_create;
  factory->destroy = librdf_hash_tokyodb_destroy;

  factory->open    = librdf_hash_tokyodb_open;
  factory->close   = librdf_hash_tokyodb_close;
  factory->clone   = librdf_hash_tokyodb_clone;

  factory->values_count = librdf_hash_tokyodb_values_count;

  factory->put     = librdf_hash_tokyodb_put;
  factory->exists  = librdf_hash_tokyodb_exists;
  factory->delete_key  = librdf_hash_tokyodb_delete_key;
  factory->delete_key_value  = librdf_hash_tokyodb_delete_key_value;
  factory->sync    = librdf_hash_tokyodb_sync;
  factory->get_fd  = librdf_hash_tokyodb_get_fd;

  factory->cursor_init   = librdf_hash_tokyodb_cursor_init;
  factory->cursor_get    = librdf_hash_tokyodb_cursor_get;
  factory->cursor_finish = librdf_hash_tokyodb_cursor_finish;
}


/**
 * librdf_init_hash_tokyodb:
 * @world: redland world object
 *
 * Initialize the Tokyo DB hash module.
 *
 **/
void
librdf_init_hash_tokyodb(librdf_world *world)
{
  librdf_hash_register_factory(world, "tokyodb", &librdf_hash_tokyodb_register_factory);
}
