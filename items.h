#ifndef ITEMS_H
#define ITEMS_H

#include "headers.h"

/*
 * Items are the data that is persisted on disk.
 * The metadata structs should not contain any pointer because they are persisted on disk.
 */

struct item_metadata {
   size_t rdt;
   size_t key_size;
   size_t value_size;
   // key
   // value
};

uint64_t item_get_rdt(char *item);
size_t get_item_size(char *item);
uint64_t item_get_key(char *item);
void* item_get_value(char *item);
uint64_t item_get_value_size(char *item);
char *clone_item(char *item);
char *next_item(char *item, size_t n);


/*
 * KVell stores binary data, but we want to store integers, sets and hashes. These functions serialize / deserialize data for KVell.
 * The end result is always a full tuple [Rdt, Size Key, Size Value, Key, Value] that KVell will blindly store.
 */

/*
 *  Item #1: a key with no value
 */
char *create_key(uint64_t uid);

/*
 * Item #2: key => uint
 */
char *create_uint_value(uint64_t uid, uint64_t val);


/*
 * Item #3: sets of uint64_t
 */
struct smember {
   size_t nb_elements;
   uint64_t elements[]; // evil array with no size because we want the data stored *in* the item, not as a pointer
};

struct smember *get_smember(char *item);
char *create_smember(uint64_t uid);
char *add_smember(char *old_item, uint64_t val, int free_item);


/*
 * Items #4: hashes of random values (int, string, ...)
 * Since hash tables are small, to simplify implementation we store them as lists [size of value, key, value, size of value, key, value].
 */
struct shash *get_shash(char *item);
uint64_t get_shash_uint(char *item, int column);
float get_shash_float(char *item, int column);
char *get_shash_string(char *item, int column);

char *add_shash_element(char *old_item, int column, void *value, size_t value_size, int free_item);
char* replace_shash_element(char *item, int column, char *value, size_t value_size, int free_item);

char *add_shash_uint(char *old_item, int column, uint64_t value, int free_item);
char *incr_shash_uint(char *old_item, int column, uint64_t amount);
char *add_shash_float(char *old_item, int column, float value, int free_item);
char *add_shash_string(char *old_item, int column, size_t min_value, size_t max_value, int free_item);
char *create_shash(uint64_t uid);
void dump_shash(char *item);

#endif
