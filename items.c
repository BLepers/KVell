#include "headers.h"

/*
 * KVell stores binary data, but we want to store integers, sets and hashes. These functions serialize / deserialize data for KVell.
 * The end result is always a full tuple [Rdt, Size Key, Size Value, Key, Value] that KVell will blindly store.
 */

// Generic function to allocate an item in memory given a value and its size
static char *_create_unique_item(uint64_t uid, void *value, size_t value_size) {
   struct item_metadata *meta;
   char *item, *item_key, *item_value;
   size_t item_size = sizeof(struct item_metadata) + sizeof(uid) + value_size;

   item = malloc(item_size);

   meta = (struct item_metadata *)item;
   meta->key_size = sizeof(uid);
   meta->value_size = value_size;

   item_key = &item[sizeof(*meta)];
   *(uint64_t*)item_key = uid;

   item_value = &item[sizeof(*meta) + meta->key_size];
   if(value)
      memcpy(item_value, value, value_size);

   return item;
}

uint64_t item_get_rdt(char *item) {
   struct item_metadata *meta = (void*)item;
   return meta->rdt;
}

size_t get_item_size(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   return sizeof(*meta) + meta->key_size + meta->value_size;
}


uint64_t item_get_key(char *item) {
   char *item_key = &item[sizeof(struct item_metadata)];
   return *(uint64_t*)item_key;
}

void* item_get_value(char *item) {
   struct item_metadata *meta = (void*)item;
   char *item_value = &item[sizeof(*meta) + meta->key_size];
   return item_value;
}

uint64_t item_get_value_size(char *item) {
   struct item_metadata *meta = (void*)item;
   return meta->value_size;
}


char *clone_item(char *item) {
   if(!item)
      return NULL;
   size_t size = get_item_size(item);
   char *new_item = malloc(size);
   memcpy(new_item, item, size);
   return new_item;
}


char *next_item(char *item, size_t n) {
   size_t size = get_item_size(item);
   char *new_item = malloc(size);
   memcpy(new_item, item, size);

   char *item_key = &new_item[sizeof(struct item_metadata)];
   *(uint64_t*)item_key = item_get_key(item) + n;

   return new_item;
}


// Item #1: a key with no value
char *create_key(uint64_t uid) {
   return _create_unique_item(uid, NULL, 0);
}

// Item #2: key => uint
char *create_uint_value(uint64_t uid, uint64_t val) {
   return _create_unique_item(uid, &val, sizeof(val));
}

/*
 * Item #3: sets of uint64_t
 */

// Format of sets stored in the KV
struct smember *get_smember(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   return (void*)&item[sizeof(*meta) + meta->key_size];
}

char *add_smember(char *old_item, uint64_t val, int free_item) {
   char *new_item;
   struct item_metadata *old_meta, *new_meta;
   size_t old_item_size, new_item_size;
   struct smember *new_set;

   old_meta = (struct item_metadata *)old_item;
   old_item_size = sizeof(struct item_metadata) + old_meta->key_size + old_meta->value_size;

   new_item_size = old_item_size + sizeof(val);
   new_item = malloc(new_item_size);
   memcpy(new_item, old_item, old_item_size);

   new_meta = (struct item_metadata *)new_item;
   new_meta->value_size += new_item_size - old_item_size;

   new_set = get_smember(new_item);
   new_set->elements[new_set->nb_elements] = val;
   new_set->nb_elements++;

   if(free_item)
      free(old_item);

   return new_item;
}

char *create_smember(uint64_t uid) {
   char *item;
   struct smember s = { . nb_elements = 0 };
   item = _create_unique_item(uid, &s, sizeof(s));
   return item;
}

/*
 * Items #4: hashes of random values (int, string, ...)
 * Since hash tables are small, to simplify implementation we store them as lists [size of value, key, value, size of value, key, value].
 */
// Format of hashes stored in the KV
struct selement {
   size_t size;
   int column;
   char value[];
};
struct shash {
   size_t nb_elements;
   char elements[];
};

struct shash *get_shash(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   return (void*)&item[sizeof(*meta) + meta->key_size];
}

static struct selement *_get_shash_element(char *item, int column, int return_last) {
   struct shash *s = get_shash(item);
   size_t nb_elements = s->nb_elements;
   size_t current_position = 0;
   struct selement *e = (void*)&s->elements[current_position];
   for(size_t i = 0; i < nb_elements; i++) {
      if(e->column == column)
         return e;
      current_position += e->size + sizeof(*e);
      e = (void*)&s->elements[current_position];
   }
   if(return_last)
      return e;
   return NULL;
}

struct selement *get_shash_element(char *item, int column) {
   return _get_shash_element(item, column, 0);
}

char* replace_shash_element(char *item, int column, char *value, size_t value_size, int free_item) {
   char *new_item;
   struct shash *new_shash, *old_shash;
   struct shash tmp_shash = { .nb_elements = 0 };

   struct selement *e = get_shash_element(item, column);
   if(!e)
      die("Cannot replace an item that is not in the hash!");

   old_shash = get_shash(item);

   new_item = _create_unique_item(item_get_key(item), NULL, item_get_value_size(item) - e->size + value_size);
   new_shash = item_get_value(new_item);
   memcpy(new_shash, &tmp_shash, sizeof(tmp_shash));


   size_t new_position = 0, old_position = 0;
   struct selement *old_element = (void*)&old_shash->elements[old_position];
   struct selement *new_element = (void*)&new_shash->elements[new_position];
   for(size_t i = 0; i < old_shash->nb_elements; i++) {
      if(old_element->column == column) { // replace
         new_element->column = column;
         new_element->size = value_size;
         memcpy(new_element->value, value, value_size);
      } else { // copy the unmodified column
         memcpy(new_element, old_element, old_element->size + sizeof(*old_element));
      }

      old_position += old_element->size + sizeof(*old_element);
      old_element = (void*)&old_shash->elements[old_position];

      new_position += new_element->size + sizeof(*new_element);
      new_element = (void*)&new_shash->elements[new_position];
      new_shash->nb_elements++;
   }

   return new_item;
}

uint64_t get_shash_uint(char *item, int column) {
   return *(uint64_t*)&_get_shash_element(item, column, 0)->value[0];
}

float get_shash_float(char *item, int column) {
   return *(float*)&_get_shash_element(item, column, 0)->value[0];
}

char *get_shash_string(char *item, int column) {
   return &_get_shash_element(item, column, 0)->value[0];
}


char *add_shash_element(char *old_item, int column, void *value, size_t value_size, int free_item) {
   char *new_item;
   struct item_metadata *old_meta, *new_meta;
   size_t old_item_size, new_item_size;
   struct shash *new_hash;
   struct selement *new_element;

   old_meta = (struct item_metadata *)old_item;
   old_item_size = sizeof(struct item_metadata) + old_meta->key_size + old_meta->value_size;

   new_item_size = old_item_size + sizeof(struct selement) + value_size;
   new_item = calloc(1, new_item_size);
   memcpy(new_item, old_item, old_item_size);

   new_meta = (struct item_metadata *)new_item;
   new_meta->value_size += new_item_size - old_item_size;

   new_hash = get_shash(new_item);
   new_element = _get_shash_element(new_item, column, 1);
   assert(new_element->size == 0); // check that the item does not exist yet
   new_element->column = column;
   new_element->size = value_size;
   memcpy(new_element->value, value, value_size);
   new_hash->nb_elements++;

   if(free_item)
      free(old_item);

   return new_item;
}

char *add_shash_uint(char *old_item, int column, uint64_t value, int free_item) {
   return add_shash_element(old_item, column, &value, sizeof(value), free_item);
}

char *incr_shash_uint(char *old_item, int column, uint64_t amount) {
   struct selement *new_element = _get_shash_element(old_item, column, 1);
   uint64_t *value = (uint64_t*)&new_element->value[0];
   *value = *value + amount;
   return old_item;
}

char *add_shash_float(char *old_item, int column, float value, int free_item) {
   return add_shash_element(old_item, column, &value, sizeof(value), free_item);
}

#define MAX_RANDOM_STRING_LEN 512
char *add_shash_string(char *old_item, int column, size_t min_value, size_t max_value, int free_item) {
   char _random_string[MAX_RANDOM_STRING_LEN];
   size_t len = rand_between(min_value, max_value);
   assert(len < MAX_RANDOM_STRING_LEN);

   for(size_t i = 0; i < len; i++)
      _random_string[i] = 'a' + rand_between(0, 26);
   _random_string[len] = 0;

   return add_shash_element(old_item, column, _random_string, len+1, free_item);
}

char *create_shash(uint64_t uid) {
   struct shash s = { .nb_elements = 0 };
   return _create_unique_item(uid, &s, sizeof(s));
}

void dump_shash(char *item) {
   struct shash *s = get_shash(item);
   size_t nb_elements = s->nb_elements;
   size_t current_position = 0;
   struct selement *e = (void*)&s->elements[current_position];
   for(size_t i = 0; i < nb_elements; i++) {
      if(e->size == sizeof(uint64_t)) { // guess it is an uint
         printf("Column %d - %lu\n", e->column, *(uint64_t*)&e->value);
      } else if(e->size == sizeof(float)) { // guess it is a float
         printf("Column %d - %f\n", e->column, *(float*)&e->value);
      } else {
         printf("Column %d - \"%s\"\n", e->column, e->value);
      }
      current_position += e->size + sizeof(*e);
      e = (void*)&s->elements[current_position];
   }
}




