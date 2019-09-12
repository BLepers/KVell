#include "headers.h"
#include "indexes/rbtree.h"

static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* In memory RB-Tree */
static rbtree *items_locations;
static pthread_spinlock_t *items_location_locks;
index_entry_t *rbtree_worker_lookup(int worker_id, void *item) {
   return rbtree_lookup(items_locations[worker_id], (void*)get_prefix_for_item(item), pointer_cmp);
}
void rbtree_worker_insert(int worker_id, void *item, index_entry_t *e) {
   pthread_spin_lock(&items_location_locks[worker_id]);
   rbtree_insert(items_locations[worker_id], (void*)get_prefix_for_item(item), e, pointer_cmp);
   pthread_spin_unlock(&items_location_locks[worker_id]);
}
void rbtree_worker_delete(int worker_id, void *item) {
   pthread_spin_lock(&items_location_locks[worker_id]);
   rbtree_delete(items_locations[worker_id], (void*)get_prefix_for_item(item), pointer_cmp);
   pthread_spin_unlock(&items_location_locks[worker_id]);
}

void rbtree_index_add(struct slab_callback *cb, void *item) {
   index_entry_t e = {
      .slab = cb->slab,
      .slab_idx = cb->slab_idx,
   };
   rbtree_worker_insert(get_worker(e.slab), item, &e);
}


/*
 * Returns up to scan_size keys >= item.key.
 * If item is not in the database, this will still return up to scan_size keys > item.key.
 */
struct index_scan rbtree_init_scan(void *item, size_t scan_size) {
   size_t nb_workers = get_nb_workers();

   struct rbtree_scan_tmp *res = malloc(nb_workers * sizeof(*res));
   for(size_t w = 0; w < nb_workers; w++) {
      pthread_spin_lock(&items_location_locks[w]);
      res[w] = rbtree_lookup_n(items_locations[w], (void*)get_prefix_for_item(item), scan_size, pointer_cmp);
      pthread_spin_unlock(&items_location_locks[w]);
   }

   struct index_scan scan_res;
   scan_res.entries = malloc(scan_size * sizeof(*scan_res.entries));
   scan_res.hashes = malloc(scan_size * sizeof(*scan_res.hashes));
   scan_res.nb_entries = 0;

   size_t *positions = calloc(nb_workers, sizeof(*positions));
   while(scan_res.nb_entries < scan_size) {
      size_t min_worker = nb_workers;
      struct rbtree_node_t *min = NULL;
      for(size_t w = 0; w < nb_workers; w++) {
         if(res[w].nb_entries <= positions[w]) {
            continue; // no more item to read in that rbtree
         } else {
            struct rbtree_node_t *current = &res[w].entries[positions[w]];
            if(!min || pointer_cmp(min->key, current->key) > 0) {
               min = current;
               min_worker = w;
            }
         }
      }
      if(min_worker == nb_workers)
         break; // no worker has any scannable item left
      positions[min_worker]++;
      scan_res.entries[scan_res.nb_entries] = min->value;
      scan_res.hashes[scan_res.nb_entries] = (uint64_t)min->key;
      scan_res.nb_entries++;
   }
   for(size_t w = 0; w < nb_workers; w++) {
      free(res[w].entries);
   }
   free(res);
   free(positions);
   return scan_res;
}

void rbtree_init(void) {
   items_locations = malloc(get_nb_workers() * sizeof(*items_locations));
   items_location_locks = malloc(get_nb_workers() * sizeof(*items_location_locks));
   for(size_t w = 0; w < get_nb_workers() ; w++) {
      items_locations[w] = rbtree_create();
      pthread_spin_init(&items_location_locks[w], PTHREAD_PROCESS_PRIVATE);
   }
}
