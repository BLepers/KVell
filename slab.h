#ifndef SLAB_H
#define SLAB_H 1

#include "ioengine.h"

struct slab;
struct slab_callback;


/* Header of a slab -- shouldn't contain any pointer as it is persisted on disk. */
struct slab {
   struct slab_context *ctx;

   size_t item_size;
   size_t nb_items;   // Number of non freed items
   size_t last_item;  // Total number of items, including freed
   size_t nb_max_items;

   int fd;
   size_t size_on_disk;

   size_t nb_free_items, nb_free_items_in_memory;
   struct freelist_entry *freed_items, *freed_items_tail;
   btree_t *freed_items_recovery, *freed_items_pointed_to;
};

/* This is the callback enqueued in the engine.
 * slab_callback->item = item looked for (that needs to be freed)
 * item = page on disk (in the page cache)
 */
typedef void (slab_cb_t)(struct slab_callback *, void *item);
enum slab_action { ADD, UPDATE, DELETE, READ, READ_NO_LOOKUP, ADD_OR_UPDATE };
struct slab_callback {
   slab_cb_t *cb;
   void *payload;
   void *item;

   // Private
   enum slab_action action;
   struct slab *slab;
   union {
      uint64_t slab_idx;
      uint64_t tmp_page_number; // when we add a new item we don't always know it's idx directly, sometimes we just know which page it will be placed on
   };
   struct lru *lru_entry;
   io_cb_t *io_cb;
};

struct slab* create_slab(struct slab_context *ctx, int worker_id, size_t item_size, struct slab_callback *callback);
struct slab* resize_slab(struct slab *s);

void *read_item(struct slab *s, size_t idx);
void read_item_async(struct slab_callback *callback);
void add_item_async(struct slab_callback *callback);
void update_item_async(struct slab_callback *callback);
void remove_item_async(struct slab_callback *callback);

off_t item_page_num(struct slab *s, size_t idx);
#endif
