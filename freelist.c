#include "headers.h"

/*
 * Freelist implementation
 * Freelists are used to store free locations on disk.
 */
struct freelist_entry {
   uint64_t slab_idx;
   uint64_t next_rdt;
   struct freelist_entry *next;
};

/*
 * Add an item to the free list. Function for items which deletion has NOT yet been persisted to disk.
 * This function is used by "not in place" updates. Rationnal is that we do not want to persit deletion of these items to disk immediately: maybe the spot can be reused to write something else (saves 1 write).
 */
void add_item_in_partially_freed_list(struct slab *s, size_t idx, size_t next_rdt) {
   struct freelist_entry *new_entry;
   new_entry = calloc(1, sizeof(*new_entry));
   new_entry->next = s->partially_freed_items;
   new_entry->slab_idx = idx;
   new_entry->next_rdt = next_rdt;
   s->partially_freed_items = new_entry;
   s->nb_partially_freed_items++;
}

/*
 * Find a free spot. Reuse partially_freed_items first.
 */
void get_free_item_idx(struct slab_callback *cb) {
   struct slab *s = cb->slab;
   struct freelist_entry *old_entry;

   if(s->nb_partially_freed_items) {
      old_entry = s->partially_freed_items;
      cb->slab_idx = old_entry->slab_idx;
      cb->lru_entry = NULL;
      cb->propagate_value_until = old_entry->next_rdt;
      s->partially_freed_items = s->partially_freed_items->next;
      s->nb_partially_freed_items--;
      free(old_entry);
      cb->io_cb(cb);
   } else {
      cb->slab_idx = -1;
      cb->lru_entry = NULL;
      cb->io_cb(cb);
      return;
   }
}
