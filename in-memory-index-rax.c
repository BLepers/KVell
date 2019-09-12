#include "headers.h"
#include "indexes/rax.h"

static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* In memory RB-Tree */
static rax **items_locations;
static pthread_spinlock_t *items_location_locks;
index_entry_t *rax_worker_lookup(int worker_id, void *item) {
   uint64_t hash = get_prefix_for_item(item);
   void *__v = raxFind(items_locations[worker_id], (unsigned char*)&(hash), sizeof(hash));
   if(__v==raxNotFound)
      return NULL;
   else
      return __v;
}
void rax_worker_insert(int worker_id, void *item, index_entry_t *e) {
   uint64_t hash = get_prefix_for_item(item);

   pthread_spin_lock(&items_location_locks[worker_id]);
   raxInsert(items_locations[worker_id],(unsigned char*)&(hash),sizeof(hash),e,NULL);
   pthread_spin_unlock(&items_location_locks[worker_id]);
}
void rax_worker_delete(int worker_id, void *item) {
   index_entry_t *old_entry = NULL;
   uint64_t hash = get_prefix_for_item(item);

   pthread_spin_lock(&items_location_locks[worker_id]);
   raxRemove(items_locations[worker_id], (unsigned char *)&(hash), sizeof(hash), (void**)(old_entry));
   pthread_spin_unlock(&items_location_locks[worker_id]);

   if(old_entry)
      free(old_entry);
}

void rax_index_add(struct slab_callback *cb, void *item) {
   index_entry_t *new_entry = malloc(sizeof(*new_entry));
   new_entry->slab = cb->slab;
   new_entry->slab_idx = cb->slab_idx;
   rax_worker_insert(get_worker(new_entry->slab), item, new_entry);
}


/*
 * Returns up to scan_size keys >= item.key.
 * If item is not in the database, this will still return up to scan_size keys > item.key.
 */
struct index_scan rax_init_scan(void *item, size_t scan_size) {
   struct index_scan scan_res;
   die("Not implemented");
   return scan_res;
}

void rax_init(void) {
   items_locations = malloc(get_nb_workers() * sizeof(*items_locations));
   items_location_locks = malloc(get_nb_workers() * sizeof(*items_location_locks));
   for(size_t w = 0; w < get_nb_workers() ; w++) {
      items_locations[w] = raxNew();
      pthread_spin_init(&items_location_locks[w], PTHREAD_PROCESS_PRIVATE);
   }
}

