#include "headers.h"

/*
 * Freelist implementation
 */

struct freelist_entry {
   uint64_t slab_idx;
   struct freelist_entry *prev;
   struct freelist_entry *next;
};

void add_item_in_free_list(struct slab *s, size_t idx, struct item_metadata *item) {
   struct freelist_entry *new_entry;
   if(s->nb_free_items_in_memory >= FREELIST_IN_MEMORY_ITEMS) {
      new_entry = s->freed_items_tail;
      item->value_size = new_entry->slab_idx;
      s->freed_items_tail = new_entry->prev;
      if(s->freed_items_tail)
         s->freed_items_tail->next = NULL;
   } else {
      new_entry = calloc(1, sizeof(*new_entry));
      item->value_size = -1;
      s->nb_free_items_in_memory++;
   }
   new_entry->slab_idx = idx;
   new_entry->next = s->freed_items;
   new_entry->prev = NULL;
   if(s->freed_items)
      s->freed_items->prev = new_entry;
   s->freed_items = new_entry;
   if(!s->freed_items_tail)
      s->freed_items_tail = new_entry;
   s->nb_free_items++;
}

void add_son_in_freelist(struct slab *s, size_t idx, struct item_metadata *item) {
   assert(s->nb_free_items_in_memory < FREELIST_IN_MEMORY_ITEMS);

   if(item->value_size != -1) {
      struct freelist_entry *new_entry = calloc(1, sizeof(*new_entry));
      new_entry->slab_idx = item->value_size;
      new_entry->next = NULL;
      new_entry->prev = s->freed_items_tail;
      if(!s->freed_items)
         s->freed_items = new_entry;
      if(s->freed_items_tail)
         s->freed_items_tail->next = new_entry;
      s->freed_items_tail = new_entry;
      s->nb_free_items_in_memory++;
   }
}

void get_free_item_idx(struct slab_callback *cb) {
   if(!cb->slab->nb_free_items_in_memory) {
      cb->slab_idx = -1;
      cb->lru_entry = NULL;
      cb->io_cb(cb);
      return;
   }

   struct slab *s = cb->slab;
   struct freelist_entry *old_entry = s->freed_items;
   cb->slab_idx = old_entry->slab_idx;
   s->freed_items = old_entry->next;
   if(s->freed_items)
      s->freed_items->prev = NULL;
   if(old_entry == s->freed_items_tail)
      s->freed_items_tail = NULL;
   s->nb_free_items--;
   s->nb_free_items_in_memory--;
   free(old_entry);
   read_page_async(cb);
}

/*
 * Debug function -- make sure all state is persisted to disk before calling it!
 * (This function uses the synchronous functions which bypass the internal page cache.)
 */
void print_free_list(struct slab *s, int depth, struct item_metadata *item) {
   if(!depth) {
      struct freelist_entry *e = s->freed_items;
      while(e) {
         struct item_metadata *i = read_item(s, e->slab_idx);
         struct item_metadata *tmp = malloc(sizeof(*tmp));
         memcpy(tmp, i, sizeof(*i));

         printf("%*.*s [IDX %lu] -> [IDX %lu]\n", depth, depth, "", e->slab_idx, i->value_size);
         print_free_list(s, depth+1, tmp);
         e = e->next;
      }
   } else if(item->value_size != -1) {
      struct item_metadata *i = read_item(s, item->value_size);
      struct item_metadata *tmp = malloc(sizeof(*tmp));
      memcpy(tmp, i, sizeof(*i));

      printf("%*.*s [IDX %lu] -> [IDX %lu]\n", depth, depth, "", item->value_size, i->value_size);
      print_free_list(s, depth+1, tmp);
   }
   if(item)
      free(item);
}

/*
 * This recovery procedure of deleted items is veeeery innefficient and would need some tuning but
 * hopefully we don't have many free spots.
 * We put all freed items and what they point to in btrees.
 * Then if an item is not pointed to, it means it should reside in memory otherwise it should reside on disk.
 * Eventually it might be faster to reset the on-disk freelist to a new free list by rewritting the tombstones as we read.
 */
void add_item_in_free_list_recovery(struct slab *s, size_t idx, struct item_metadata *item) {
   /* If you get some sigsev in this function it is probably because the recovery procedure of freed item is memory intensive.
    * This could be avoided but we didn't really pay to much attention to it (YCSB workloads don't delete...) */
   size_t son_idx = item->value_size;
   if(!s->freed_items_recovery) {
      s->freed_items_recovery = btree_create();
      s->freed_items_pointed_to = btree_create();
   }

   struct index_entry freed_entry = {
      .slab_idx = son_idx
   };
   btree_insert(s->freed_items_recovery, (unsigned char*)(&idx), sizeof(idx), &freed_entry);

   if(item->value_size != -1) {
      struct index_entry pointed_to_entry = {};
      btree_insert(s->freed_items_pointed_to, (unsigned char*)(&son_idx), sizeof(son_idx), &pointed_to_entry);
   }

   s->nb_free_items++;
}

static void btree_iterator(uint64_t h, void *data) {
   struct slab *s = data;
   struct index_entry e;
   if(!btree_find(s->freed_items_pointed_to, (unsigned char*)(&h), sizeof(h), &e)) {
      struct freelist_entry *new_entry = calloc(1, sizeof(*new_entry));
      new_entry->slab_idx = h;
      new_entry->next = s->freed_items;
      if(s->freed_items)
         s->freed_items->prev = new_entry;
      s->freed_items = new_entry;
      if(!s->freed_items_tail)
         s->freed_items_tail = new_entry;
      s->nb_free_items_in_memory++;
   }
}

void rebuild_free_list(struct slab *s) {
   if(s->freed_items_recovery) {
      btree_forall_keys(s->freed_items_recovery, btree_iterator, s);
      btree_free(s->freed_items_pointed_to); // TODO: check that this is freeing all the memory
      btree_free(s->freed_items_recovery);
   }
}
