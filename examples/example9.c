/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * example9.c - Redland example parsing RDF from a URI, storing on disk as Tokyo DB hashes and querying the results
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define DB_NAME_SUFFIX "db"

#include <redland.h>

#define STRESS_TEST_ALL_ITERATION 1
#define STRESS_TEST_HASH_ITERATION 1
#define STRESS_TEST_PUT_ITERATION 1

static int delete_key_value (librdf_world *world, librdf_hash *h, const char* key, const char* value, librdf_hash_datum *hd_key, librdf_hash_datum *hd_value) {
  int ret = -1;
  /* Delete key value */
  hd_key->data=(char*)key;
  hd_key->size=strlen((char*)hd_key->data);

  if (value != NULL && hd_value != NULL) {
    hd_value->data=(char*)value;
    hd_value->size=strlen((char*)hd_value->data);
    ret = librdf_hash_delete(h, hd_key, hd_value);
  }
  else {
    /* delete all keys */
    ret = librdf_hash_delete_all(h, hd_key);
  }

  return ret;
}

static int test_delete(librdf_world *world, librdf_hash *h, const char **test_delete_array, int test_delete_array_len) {
  librdf_hash_datum hd_key, hd_value;

  int j;
  for(j=0; j<test_delete_array_len; j+=2) {
    const char *key = (char*)test_delete_array[j];
    const char *value = (char*)test_delete_array[j+1];
    int ret = -1;
    if (key) {
      if (!value) {
        fprintf(stdout, "Deleting key '%s' \n", key);
        ret = delete_key_value(world, h, key, NULL, &hd_key, NULL);
      }
      else {
        fprintf(stdout, "Deleting key->value '%s' -> '%s' \n", key, value);
        ret = delete_key_value(world, h, key, value, &hd_key, &hd_value);
      }

      if (!ret) {
        fprintf(stdout, "delete success. total values: %d.", librdf_hash_values_count(h));
        fprintf(stdout, "\nresulting hash: ");
        librdf_hash_print(h, stdout);
        fputc('\n', stdout);
      }
      else
        fprintf(stdout, "delete failed: %d\n", ret);
    }
  }

  return 0;
}

static int test_put(librdf_world *world, librdf_hash *h, const char **test_put_array, int test_put_array_len) {

  librdf_hash_datum hd_key, hd_value;

  int j;
  for(j=0; j<test_put_array_len; j+=2) {
    const char *key = (char*)test_put_array[j];
    const char *value = (char*)test_put_array[j+1];

    int ret = -1;
    if (key && value) {
      hd_key.data=(char*)key;
      hd_value.data=(char*)value;
      hd_key.size=strlen((char*)hd_key.data);
      hd_value.size=strlen((char*)hd_value.data);
      ret = librdf_hash_put(h, &hd_key, &hd_value);

      if (ret)
        fprintf(stdout, "put failed: %d", ret);
    }
  }

  return 0;
}

static int test_string_manipulation(librdf_world *world, librdf_hash *h) {

  const char *test_hash_array[]={"shape", "cube",
      "sides", "6", /* for testing get as long */
      "3d", "yes", /* testing bool */
      "colour", "red",
      "colour", "yellow",
      "creator", "rubik",
      NULL};

  const unsigned char* template_string=(const unsigned char*)"the shape is %{shape} and the sides are %{sides} created by %{creator}";
  const unsigned char* template_expected=(const unsigned char*)"the shape is cube and the sides are 6 created by rubik";
  const char * const test_hash_string="field1='value1', field2='\\'value2', field3='\\\\', field4='\\\\\\'', field5 = 'a' ";
  const char * filter_string[] = {"field1", NULL};

  librdf_iterator* iterator;
  librdf_hash_datum *key_hd;
  char *string_result;
  unsigned char *template_result;
  int b;
  long l;

  /*
   *
   * Test librdf_hash_from_array_of_strings
   *
   */
  fprintf(stdout, "Initializing hash from array of strings\n");
  if(librdf_hash_from_array_of_strings(h, test_hash_array)) {
    fprintf(stderr, "Failed to init hash from array of strings\n");
    return(1);
  }

  fprintf(stdout, "librdf_hash_from_array_of_strings success. total values: %d.", librdf_hash_values_count(h));
  fprintf(stdout, "\nresulting hash: ");
  librdf_hash_print(h, stdout);
  fputc('\n', stdout);

  fprintf(stdout, "\nresulting hash keys: ");
  librdf_hash_print_keys(h, stdout);
  fputc('\n', stdout);

  /*
   *
   * Test librdf_hash_get_as_boolean and librdf_hash_get_as_long
   *
   */
  key_hd=librdf_new_hash_datum(world, NULL, 0);

  iterator=librdf_hash_keys(h, key_hd);
  while(!librdf_iterator_end(iterator)) {
    librdf_hash_datum *k=(librdf_hash_datum*)librdf_iterator_get_key(iterator);
    char *key_string;

    key_string = LIBRDF_MALLOC(char*, k->size + 1);
    if(!key_string)
      break;
    strncpy(key_string, (char*)k->data, k->size);
    key_string[k->size]='\0';

    fprintf(stdout, "boolean value of key '%s' is ", key_string);
    b=librdf_hash_get_as_boolean(h, key_string);
    fprintf(stdout, "%d (0 F, -1 Bad, else T)\n", b);

    fprintf(stdout, "long value of key '%s' is ", key_string);
    l=librdf_hash_get_as_long(h, key_string);
    fprintf(stdout, "%ld (decimal, -1 Bad)\n", l);

    LIBRDF_FREE(char*, key_string);
    librdf_iterator_next(iterator);
  }
  if(iterator)
    librdf_free_iterator(iterator);
  librdf_free_hash_datum(key_hd);


  /*
   *
   * Test librdf_hash_from_string
   *
   */
  fprintf(stdout, "Initializing hash from string >>%s<<\n", test_hash_string);
  librdf_hash_from_string (h, test_hash_string);

  fprintf(stdout, "values count %d\n", librdf_hash_values_count(h));
  fprintf(stdout, "resulting: ");
  librdf_hash_print(h, stdout);
  fputc('\n', stdout);


  fprintf(stdout, "Converting hash back to a string");
  string_result=librdf_hash_to_string(h, NULL);

  /* Order is not guaranteed, so sadly we can't just do a full string comparison */
  if(!strstr(string_result, "field1='value1'")) {
    fprintf(stdout, "Did not see field1='value1' in the generated string >>%s<<\n",
        string_result);
    return 0;
  } else if(!strstr(string_result, "field2='\\'value2'")) {
    fprintf(stdout, "Did not see field2='\\'value2'' in the generated string >>%s<<\n",
        string_result);
    return 0;
  } else if(!strstr(string_result, "field3='\\\\'")) {
    fprintf(stdout, "Did not see field3='\\\\' in the generated string >>%s<<\n",
        string_result);
    return 0;
  } else if(!strstr(string_result, "field4='\\\\\\'")) {
    fprintf(stdout, "Did not see field4='\\\\\\' in the generated string >>%s<<\n",
        string_result);
    return 0;
  } else if(!strstr(string_result, "field5='a'")) {
    fprintf(stdout, "Did not see field5='a' in the generated string >>%s<<\n", string_result);
    return 0;
  } else {
    fprintf(stdout, "\nresulting in >>%s<<\n", string_result);
  }
  librdf_free_memory(string_result);

  fprintf(stdout, "Converting hash back to a string with filter");
  string_result=librdf_hash_to_string(h, filter_string);
  if(strstr(string_result, "field1")) {
    fprintf(stdout, "Was not expecting >>field1<< to be in the generated string >>%s<<\n",
        string_result);
    return 0;
  } else {
    fprintf(stdout, "\nresulting in >>%s<<\n", string_result);
  }
  librdf_free_memory(string_result);

  /*
   *
   * Test librdf_hash_interpret_template
   *
   */
  fprintf(stdout, "Substituting into template >>%s",
      template_string);
  template_result=librdf_hash_interpret_template(template_string, h,
      (const unsigned char*)"%{",
      (const unsigned char*)"}");
  if(strcmp((const char*)template_result, (const char*)template_expected)) {
    fprintf(stdout, "Templating failed. Result was >>%s<< but expected >>%s<<\n",
        template_result, template_expected);
    return 0;
  } else
    fprintf(stdout, "\nresulting in >>%s<<\n", template_result);

  LIBRDF_FREE(char*, template_result);

  return 0;
}

static int test_hash_funtionality(librdf_world *world, librdf_hash *h) {
        librdf_hash *ch;
  const char *test_put_array[]={
      "colour","yellow",
      "age", "new",
      "size", "large",
      "colour", "green",
      "fruit", "banana",
      "colour", "yellow",
  };

  const char *test_delete_array[]={
      "invalidkey", "invalidvalue",
      "colour", "yellow",
      "colour", "aaaaaaaaaaaaainvalidvalue",
      "colour", "zzzzzzzzzzzzzinvalidvalue",
      "colour", NULL,
      "fruit", NULL,
      "size", "large",
      "age", "new",
  };

  const char *test_get_values_for_key="colour";
  int len, i;

  for (i=1; i<=STRESS_TEST_PUT_ITERATION; i++) {
    fprintf(stdout, "put iteration.. %d\n", i);

    /* Test put */
    len = sizeof(test_put_array)/sizeof(const char*);
    test_put(world, h, test_put_array, len);
  }

  fprintf(stdout, "total values: %d.", librdf_hash_values_count(h));

  /* Test get all keys only */
  fprintf(stdout, "all hash keys:");
  librdf_hash_print_keys(h, stdout);
  fputc('\n', stdout);

  /* Test get all values of given key */
  fprintf(stdout, "all values of key '%s'=", test_get_values_for_key);
  librdf_hash_print_values(h, test_get_values_for_key, stdout);
  fputc('\n', stdout);

  /* Test cloning hash */
  fprintf(stdout, "cloning hash\n");
  ch = librdf_new_hash_from_hash(h);
  if(ch) {
    fprintf(stdout, "clone success. values count %d\n", librdf_hash_values_count(ch));
    fprintf(stdout, "resulting: ");
    librdf_hash_print(ch, stdout);
    fputc('\n', stdout);

    librdf_hash_close(ch);
    librdf_free_hash(ch);
  } else {
    fprintf(stderr, "Failed to clone hash\n");
  }

  /* Test delete */
  len = sizeof(test_delete_array)/sizeof(const char*);
  test_delete(world, h, test_delete_array, len);

  /* Test string related features */
  test_string_manipulation(world, h);

  return 0;
}

static void test_all(const char *program) {
  librdf_hash *h;
  int num_test_hash_types, i, j;
  //const char *test_hash_types[]={"bdb", "tokyodb", "memory", NULL};
  const char *test_hash_types[]={"tokyodb"};
  librdf_world *world;

  world=librdf_new_world();
  librdf_world_open(world);

  num_test_hash_types = sizeof(test_hash_types)/sizeof(const char*);
  for(i=0; i < num_test_hash_types; i++) {
    char db_name[100];

    const char *type = test_hash_types[i];
    fprintf(stdout, "Trying to create new %s hash\n", type);
    h=librdf_new_hash(world, type);
    if(!h) {
      fprintf(stderr, "Failed to create new hash type '%s' '%s'\n", program, type);
      continue;
    }

    sprintf(db_name, "test_%s_%s", type, DB_NAME_SUFFIX);
    if(librdf_hash_open(h, db_name, 0644, 1, 1, NULL)) {
      fprintf(stderr, "Failed to open new hash type '%s' '%s'\n", program, type);
      continue;
    }

    for (j=1; j<=STRESS_TEST_HASH_ITERATION; j++) {
      fprintf(stdout, "test_hash_funtionality iteration.. %d\n", j);
      test_hash_funtionality(world, h);
    }

    librdf_hash_close(h);
    fprintf(stdout, "Freeing hash: %s\n", program);
    librdf_free_hash(h);
  }

  librdf_free_world(world);
}

int
main(int argc, char *argv[]) {
  int j;
  for (j=1; j<=STRESS_TEST_ALL_ITERATION; j++) {
    fprintf(stdout, "test_all iteration.. %d\n", j);
    test_all((const char*)argv[0]);
  }

  return 0;
}
