#include "headers.h"

/*
 * "Garbage collector" for old versions of items stored in snapshots.
 * We have a list per worker.
 * When an item is updated, it is added in the list.
 * After submitting IOs, the worker scans the list and delete snapshots that can no longer be read.
 *    No longer be read = the timestamp of the *next* version of the element is > min(snapshot id) of active transactions
 *    I.e., all transactions can read a version that is more recent than the version that we are looking at.
 */

//#define MAXIMUM_GC_ELEMENTS 10000000
#define MAXIMUM_GC_ELEMENTS 100000000

struct element_to_be_freed;
struct to_be_freed_list {
   struct element_to_be_freed *elements;
   size_t head, tail;
   size_t max_avail;
};

#define tail(r) ((r).tail % (r).max_avail)
#define head(r) ((r).head % (r).max_avail)


#if TRANSACTION_TYPE == TRANS_LONG
struct element_to_be_freed {
   struct slab *s;
   size_t idx;
   uint64_t rdt;
};
#else
struct element_to_be_freed {
   uint64_t hash;
   uint64_t rdt;
};
#endif

size_t gc_size(struct to_be_freed_list *l) {
   return l->tail - l->head;
}

static maybe_unused uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* Delete all versions < snapshot_id */
void do_deletions(uint64_t worker_id, struct to_be_freed_list *l) {
   assert(is_worker_context());

   if(l->tail == l->head)
      return;

   size_t nb_deletions = 0;
   struct element_to_be_freed *e;
   uint64_t snapshot_id;
   if(TRANSACTION_TYPE == TRANS_LONG)
     snapshot_id = get_min_in_commit();
   else
     snapshot_id = get_min_snapshot_id();

   //printf("Cleaning till %lu\n", snapshot_id);
   do {
      e = &l->elements[head(*l)];
      if(e->rdt >= snapshot_id) // The list is not fully ordered, but at some point this will become true, so stop there
         break;
#if TRANSACTION_TYPE == TRANS_LONG
      e->s->nb_items--;
      add_item_in_partially_freed_list(e->s, e->idx, e->rdt);
#else
      memory_index_clean_old_versions(worker_id, e->hash, snapshot_id);
#endif
      l->head++;
      if(l->head == l->tail)
         break;
      nb_deletions++;
      if(nb_deletions > MAX_CLEANING_OP_PER_ROUND)
         break;
   } while(1);
}

/* Enqueue an item in the list */
static void put_element_in_wait_to_be_freed_list(struct to_be_freed_list *l, char *item, uint64_t index_rdt) {
#if TRANSACTION_TYPE != TRANS_LONG
   struct element_to_be_freed *e = &l->elements[tail(*l)];
   e->hash = get_prefix_for_item(item);
   e->rdt = index_rdt;
   l->tail++;
   if(tail(*l) == head(*l))
      die("Maximum number of old versions exceeded, increase MAXIMUM_GC_ELEMENTS\n");
#endif
}

static void put_location_in_wait_to_be_freed_list(struct to_be_freed_list *l, struct slab *s, size_t idx, uint64_t index_rdt) {
#if TRANSACTION_TYPE == TRANS_LONG
   struct element_to_be_freed *e = &l->elements[tail(*l)];
   e->s = s;
   e->idx = idx;
   e->rdt = index_rdt;
   l->tail++;
   if(tail(*l) == head(*l))
      die("Maximum number of old versions exceeded, increase MAXIMUM_GC_ELEMENTS\n");
#endif
}

/* Add an item in the list of the worker */
void add_item_in_gc(struct to_be_freed_list *l, struct slab_callback *callback, uint64_t index_rdt) {
   if(!callback->old_slab) // Do not put in GC some location that didn't exist!
      return;
   if(TRANSACTION_TYPE == TRANS_FAST || get_nb_running_transactions() == 0) { // no running transaction
      struct slab *s = callback->old_slab;
      size_t idx = callback->old_slab_idx;
      s->nb_items--;
      add_item_in_partially_freed_list(s, idx, index_rdt);
   } else if(TRANSACTION_TYPE == TRANS_LONG) {
      struct slab *s = callback->old_slab;
      size_t idx = callback->old_slab_idx;
      put_location_in_wait_to_be_freed_list(l, s, idx, index_rdt);
   } else if(TRANSACTION_TYPE == TRANS_SNAPSHOT) {
      put_element_in_wait_to_be_freed_list(l, callback->item, index_rdt);
   }
}

struct to_be_freed_list *init_gc(void) {
   struct to_be_freed_list *l = calloc(1, sizeof(*l));
   l->elements = calloc(MAXIMUM_GC_ELEMENTS, sizeof(*l->elements));
   l->max_avail = MAXIMUM_GC_ELEMENTS;
   return l;
}

