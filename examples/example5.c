/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * example5.c - Redland example code using querying
 *
 * $Id$
 *
 * Copyright (C) 2004 David Beckett - http://purl.org/net/dajobe/
 * Institute for Learning and Research Technology - http://www.ilrt.org/
 * University of Bristol - http://www.bristol.ac.uk/
 * 
 * This package is Free Software or Open Source available under the
 * following licenses (these are alternatives):
 *   1. GNU Lesser General Public License (LGPL)
 *   2. GNU General Public License (GPL)
 *   3. Mozilla Public License (MPL)
 * 
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * full license terms.
 * 
 * 
 */


#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>

#include <redland.h>

/* one prototype needed */
int main(int argc, char *argv[]);


int
main(int argc, char *argv[]) 
{
  char *program=argv[0];
  librdf_world* world;
  librdf_storage *storage;
  librdf_model* model;
  const char *parser_name;
  librdf_parser* parser;
  librdf_query* query;
  librdf_query_results* results;
  librdf_uri *uri;
  char *query_string=NULL;
  
  world=librdf_new_world();
  librdf_world_open(world);

  if(argc !=3) {
    fprintf(stderr, "USAGE: %s CONTENT-URI QUERY-STRING\n", program);
    return 1; 
  }
  
  uri=librdf_new_uri(world, argv[1]);
  query_string=argv[2];
  
  model=librdf_new_model(world, storage=librdf_new_storage(world, "hashes", "test", "new='yes',hash-type='bdb',dir='.'"), NULL);
  if(!model || !storage) {
    fprintf(stderr, "%s: Failed to make model or storage\n", program);
    return 1;
  }

  parser_name=raptor_guess_parser_name(NULL, NULL, NULL, 0, librdf_uri_as_string(uri));
  parser=librdf_new_parser(world, parser_name, NULL, NULL);
  librdf_parser_parse_into_model(parser, uri, NULL, model);
  librdf_free_parser(parser);
  librdf_free_uri(uri);

  query=librdf_new_query(world, "rdql", NULL, query_string);
  
  results=librdf_model_query_execute(model, query);
  if(!results) {
    fprintf(stderr, "%s: Query of model with '%s' failed\n", 
            program, query_string);
    return 1;
  }

  while(!librdf_query_results_finished(results)) {
    const char **names=NULL;
    librdf_node* values[10];
    
    if(librdf_query_results_get_bindings(results, &names, values))
      break;
    
    fputs("result: [", stdout);
    if(names) {
      int i;
      
      for(i=0; names[i]; i++) {
        fprintf(stdout, "%s=", names[i]);
        if(values[i]) {
          librdf_node_print(values[i], stdout);
          librdf_free_node(values[i]);
        } else
          fputs("NULL", stdout);
        if(names[i+1])
          fputs(", ", stdout);
      }
    }
    fputs("]\n", stdout);
    
    librdf_query_results_next(results);
  }
  
  fprintf(stdout, "%s: Query returned %d results\n", program, 
          librdf_query_results_get_count(results));

  librdf_free_query_results(results);
  librdf_free_query(query);
  
  librdf_free_model(model);
  librdf_free_storage(storage);

  librdf_free_world(world);

#ifdef LIBRDF_MEMORY_DEBUG
  librdf_memory_report(stderr);
#endif
	
  /* keep gcc -Wall happy */
  return(0);
}
