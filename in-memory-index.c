#include "headers.h"

static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

// TODO: do a per thread structure!
static btree_t **items_locations;               // Main index
static btree_t **old_items_locations;           // For snapshot isolation, old versions still stored on disk
static size_t *nb_snapshotted_items;


/*
 * Lookup an item in the snapshots
 */
index_entry_t *memory_index_lookup_old_version_for_read(int worker_id, uint64_t hash, uint64_t snapshot_id) {
   assert(is_worker_context());

   if(TRANSACTION_TYPE != TRANS_SNAPSHOT && TRANSACTION_TYPE != TRANS_LONG) // Snapshot not supported!
      return NULL;

   index_entry_t *mvcc;
   int res = btree_find(old_items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &mvcc);
   if(res) {
      index_entry_t *max_readable = NULL;
      for(size_t i = 0; i < mvcc->nb_versions; i++) {
         index_entry_t *tmp = &mvcc->versions[i];
         //printf("Found old version of hash %lu (%p) -- %lu < %lu && %lu > %lu?\n", hash, mvcc, get_rdt_value(tmp), snapshot_id, get_rdt_value(tmp), get_rdt_value(&max_readable));
         // We try to find the most recent version that we can read (rdt < our transaction id)
         if(get_rdt_value(tmp) <= snapshot_id && (!max_readable || get_rdt_value(tmp) > get_rdt_value(max_readable))) {
            max_readable = tmp;
         }
      }
      if(max_readable && max_readable->slab) { // We found an old version and it is not a "fake old version" (see _memory_index_clean_old_versions)
         if(snapshot_id >= mvcc->current_rdt) { // special case, a fake old version existed at the end, but it has been deleted!
            // Logic of the deletion:
            //   before: mvcc [current_rdt = 1000] [ 2, 5, 10]
            //   after:  mvcc [current_rdt = 10] [2, 5]
            // The loop before says we can read 5, but we should have read 10, hadn't it been reused. We remember that by looking at the current_rdt.
         } else {
            return max_readable;
         }
      }
   }
   //printf("Couln't find suitable version %lu %lu versions snapshot_id %lu\n", hash, mvcc?mvcc->nb_versions:0, snapshot_id);
   return NULL;
}

/*
 * Lookup an item in the main memory index
 */
index_entry_t *memory_index_lookup(int worker_id, struct slab_callback *cb, void *item, uint64_t snapshot_id, int *allowed) {
   assert(is_worker_context());

   index_entry_t *result = NULL;
   uint64_t hash = get_prefix_for_item(item);
   int res = btree_find(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &result);
   if(res) {
      if(cb && !action_allowed(result, cb)) {
         result = memory_index_lookup_old_version_for_read(worker_id, hash, snapshot_id);
         if(result) {
            if(allowed)
               *allowed = 1;
            return result;
         } else {
            if(allowed)
               *allowed = 0;
            return NULL;
         }
      }
      if(allowed)
         *allowed = 1;
      return result;
   } else {
      if(allowed)
         *allowed = 1;
      return NULL;
   }
}


/*
 * Lookup next item.
 */
index_entry_t *_memory_index_lookup_next(int worker_id, struct slab_callback *cb, uint64_t hash, uint64_t *found_hash, uint64_t snapshot_id) {
   assert(is_worker_context());

   int res;
   index_entry_t *result;

   //printf("Asking me for %lu to %lu\n", hash, cb->max_next_key);
again:
   res = btree_find_next(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &result, found_hash);
   if(res && *found_hash < cb->max_next_key) {
      if(!action_allowed(result, cb)) {
         result = memory_index_lookup_old_version_for_read(worker_id, *found_hash, snapshot_id);
         if(!result) {
            //printf("Trans %lu snap %lu not allowed to read %lu\n", get_transaction_id(cb->transaction), snapshot_id, *found_hash);
            hash = *found_hash;
            goto again;
         }
      }
      //printf("Trans %lu snap %lu will read %lu (rdt %lu)\n", get_transaction_id(cb->transaction), snapshot_id, *found_hash, get_rdt_value(result));
      return result;
   } else {
      return NULL;
   }
}

index_entry_t *memory_index_lookup_next(int worker_id, struct slab_callback *cb, void *item, uint64_t *found_hash, uint64_t snapshot_id, int *allowed) {
   uint64_t hash = get_prefix_for_item(item);
   *allowed = 1;
   return _memory_index_lookup_next(worker_id, cb, hash, found_hash, snapshot_id);
}

/*
 * Lookup next batch
 */
index_entry_t *memory_index_read_next_batch(int worker_id, struct slab_callback *cb, void *item, uint64_t snapshot_id, size_t *batch_size, uint64_t **hashes, int *allowed) {
   assert(is_worker_context());

   size_t desired_size = *batch_size, actual_size = 0;
   index_entry_t *res = calloc(desired_size, sizeof(*res));
   *hashes = calloc(desired_size, sizeof(**hashes));
   *allowed = 1;

   uint64_t last_hash = get_prefix_for_item(item), new_hash;
   while(actual_size < desired_size) {
      index_entry_t *e = _memory_index_lookup_next(worker_id, cb, last_hash, &new_hash, snapshot_id);
      if(e) {
         (*hashes)[actual_size] = new_hash;
         assert(new_hash);
         res[actual_size] = *e;
         actual_size++;
         last_hash = new_hash;
      } else {
         break;
      }
   }
   *batch_size = actual_size;
   return res;
}

static void _memory_index_clean_old_versions(int worker_id, uint64_t hash, uint64_t snapshot_id, int specific) {
   assert(is_worker_context());

   size_t last_index = 0;

   index_entry_t *mvcc;
   int res = btree_find(old_items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &mvcc);
   if(specific)
      assert(res);
   if(!res)
      return;

   //index_entry_t *new;
   //int exists = btree_find(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &new);
   //assert(exists);
   //printf("Cleaning key %lu - %lu versions - %lu last rdt, clean up until %lu\n", hash, mvcc->nb_versions, mvcc->current_rdt, snapshot_id);
   int previous_is_empty = 0;
   for(size_t j = 0; j < mvcc->nb_versions; j++) {
      index_entry_t *e = &mvcc->versions[j];

      uint64_t next_rdt;
      // Only remove the item if the *next* item is readable by snapshot id
      if(j < mvcc->nb_versions - 1) {
         next_rdt = get_rdt_value(&mvcc->versions[j+1]);
      } else {
         next_rdt = mvcc->current_rdt;
      }

      if(specific && get_rdt_value(e) == snapshot_id) {
         //printf("\tRemoving key %lu - %lu from old versions because rdt %lu matches %lu\n", hash, get_rdt_value(e), get_rdt_value(e), snapshot_id);
         /*
          * For correctness we need to create "fake old entries":
          *    t0:  MVCC = [ rdt0, rdt1, rdt2 ]
          *    t1:  we propagate rdt1 and subsequently remove it from MVCC = [rdt0, rdt2]
          *    t2:  we do a scan of the DB. We might end up returning rdt0 if we don't remember rdt1 was there!
          * ==> for correctness we keep rdt1 as long as it is required!
          */
         e->slab = NULL; // mark the element as deleted, but don't remove the spot
         nb_snapshotted_items[worker_id]--;
         if(j == 0
               || j == mvcc->nb_versions - 1
               || previous_is_empty) { // element is at beginning, end, or previous is already empty ==> delete the spot
            if(j == mvcc->nb_versions - 1) { // if element is last, we need to modify the "current version"
               mvcc->current_rdt = get_rdt_value(e);
               if(previous_is_empty) { // also delete previous
                  mvcc->current_rdt = get_rdt_value(&mvcc->versions[j-1]);
                  last_index--;
               }
            }
         } else {
            last_index++; // leave the spot as is
         }
         previous_is_empty = 1;
      } else if(!specific && next_rdt < snapshot_id) {
         //printf("\tRemoving %lu - %lu from old versions because next rdt %lu (newest rdt is %lu but we know %lu)\n", hash, get_rdt_value(e), next_rdt, get_rdt_value(new), mvcc->current_rdt);
         struct slab *s = e->slab;
         size_t idx = e->slab_idx;
         s->nb_items--;
         add_item_in_partially_freed_list(s, idx, next_rdt);
         nb_snapshotted_items[worker_id]--;
      } else {
         //printf("\tKeeping key %lu - %lu from old versions because next rdt %lu != snapshot_id %lu\n", hash, get_rdt_value(e), next_rdt, snapshot_id);
         if(previous_is_empty && !e->slab) { // previous is already a fake spot, let's not have 2 fake spots in a row
            // do nothing
         } else {
            previous_is_empty = !e->slab; // remember if the copied element is not fake
            mvcc->versions[last_index] = *e;
            last_index++;
         }
      }
   }
   if(last_index == 0) { // no old version left!
      free(mvcc->versions);
      btree_delete(old_items_locations[worker_id], (unsigned char*)&hash, sizeof(hash));
      //printf("Deleting key %lu from old btree\n", hash);
   } else {
      //printf("key %lu still has %lu versions (%lu before) snapshot_id %lu oldest %lu newest %lu\n", hash, last_index, mvcc->nb_versions, snapshot_id, get_rdt_value(&mvcc->versions[0]), mvcc->current_rdt);
      mvcc->nb_versions = last_index;
      //for(size_t j = 0; j < mvcc->nb_versions; j++)
         //printf("\tLeft: key %lu - %lu (deleted: %s)\n", hash, get_rdt_value(&mvcc->versions[j]), mvcc->versions[j].slab?"no":"yes");
   }
}
void memory_index_clean_old_versions(int worker_id, uint64_t hash, uint64_t snapshot_id) {
   _memory_index_clean_old_versions(worker_id, hash, snapshot_id, 0);
}
void memory_index_clean_specific_version(void *item) {
   _memory_index_clean_old_versions(get_worker_for_item(item), get_prefix_for_item(item), item_get_rdt(item), 1);
}

void memory_index_revert(int worker_id, void *item, struct slab_callback *cb, uint64_t transaction_id) {
   assert(is_worker_context());

   index_entry_t *new;
   uint64_t hash = get_prefix_for_item(item);


   /* Get the pointer to the item in the main index */
   int exists = btree_find(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &new);
   assert(exists);

   if(!get_locked_bit(new))
      die("Reverting an item that is not locked?? %lu\n", hash);

   if(!new->slab) { // Item was reserved but not committed, so deleted it
      //printf("Deleting %lu\n", hash);
      btree_delete(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash));
   } else { // Reverting a commit simply means unlocking the item
      //printf("Unlocking %lu\n", hash);
      unset_locked_bit(new);
   }
}


/*
 * For writes, we lock the item in memory and maybe add the old version in the snapshots
 */

/* Lock an item in the index */
static void memory_index_lock(index_entry_t *e, uint64_t transaction_id) {
   assert(!get_locked_bit(e)); // Somebody else locked the item before, we shouldn't be able to lock it again!
   //e->rdt = transaction_id; // Do NOT modify the RDT of he item, we still want to be able to read the old value!
   set_locked_bit(e);
}

/* Put the old location in the snapshot index and lock the item in the main index */
static void memory_index_snapshot(int worker_id, void *item, index_entry_t *e, uint64_t transaction_id) {
   assert(is_worker_context());

   /* Remember the old location of the item in the "old_items_locations" tree */
   index_entry_t *mvcc;
   index_entry_t new_mvcc;
   uint64_t hash = get_prefix_for_item(item);
   int had_old_version_already = btree_find(old_items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &mvcc);
   if(!had_old_version_already) {
      //printf("Key %lu - %lu; creating snapshots newest %lu!!\n", hash, get_rdt_value(e), transaction_id);
      new_mvcc.versions = NULL;
      new_mvcc.nb_versions = 0;
      mvcc = &new_mvcc;
   } else if(mvcc->current_rdt != get_rdt_value(e)) { // need to recreate a fake old version!!
      //printf("Key %lu - %lu; creating snapshots with fake %lu %lu newest %lu!!\n", hash, get_rdt_value(e), mvcc->current_rdt, get_rdt_value(e), transaction_id);
      mvcc->versions = realloc(mvcc->versions, (mvcc->nb_versions + 1) * sizeof(*mvcc->versions));
      mvcc->versions[mvcc->nb_versions].slab = NULL;
      mvcc->versions[mvcc->nb_versions].rdt = mvcc->current_rdt;
      mvcc->nb_versions++;
   } else {
      //printf("Key %lu - %lu; creating snapshots no fake %lu %lu newest %lu!!\n", hash, get_rdt_value(e), mvcc->current_rdt, get_rdt_value(e), transaction_id);
   }
   mvcc->current_rdt = transaction_id;
   mvcc->versions = realloc(mvcc->versions, (mvcc->nb_versions + 1) * sizeof(*mvcc->versions));
   mvcc->versions[mvcc->nb_versions] = *e;
   mvcc->nb_versions++;
   if(!had_old_version_already) {
      btree_insert(old_items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), mvcc);
   }
   nb_snapshotted_items[worker_id]++;
}

/* Lookup an item, and maybe lock it */
index_entry_t *memory_index_lookup_and_lock(int worker_id, void *item, struct slab_callback *cb, uint64_t transaction_id, int *present, int *allowed) {
   assert(is_worker_context());

   index_entry_t *result;
   uint64_t hash = get_prefix_for_item(item);
   int res = btree_find(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &result);
   if(res) {
      *present = 1;
      if(action_allowed(result, cb)) {
         *allowed = 1;
         //printf("Locking %lu by %lu\n", hash, transaction_id);
         memory_index_lock(result, transaction_id);
         return result;
      } else {
         *allowed = 0;
         return NULL;
      }
   } else {
      *present = 0; // item is not here
      *allowed = 1; // but we are allowed to create it
      return NULL;
   }
}

uint64_t memory_item_update_remove_lock(struct slab_callback *cb, void *item) {
   assert(is_worker_context());

   uint64_t rdt;
   index_entry_t *e;
   int worker_id = get_worker_for_item(item);
   uint64_t hash = get_prefix_for_item(item);

   int res = btree_find(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), &e);
   if(res) {
      if(!get_locked_bit(e) && cb->transaction) {
         die("Double unlock?! -- likely a bug\n");
      } else {
         // Remember the old location, useful for TRANS_FAST that does not keep a snapshot but deletes straight away
         cb->old_slab = e->slab;
         cb->old_slab_idx = e->slab_idx;
         if((TRANSACTION_TYPE == TRANS_SNAPSHOT || TRANSACTION_TYPE == TRANS_LONG) && e->slab) // if item existed (i.e., was not just reserved), then snapshot the old version
            memory_index_snapshot(worker_id, item, e, item_get_rdt(item));
         e->slab = cb->slab;
         e->slab_idx = cb->slab_idx;
         e->rdt = item_get_rdt(item);
         rdt = e->rdt;
         //printf("Unlocking %lu\n", hash);
      }
   } else {
      die("Cannot unlock because it does not exist?!\n");
   }

   return rdt;
}

static void memory_index_insert(int worker_id, void *item, index_entry_t *e) {
   assert(is_worker_context());

   uint64_t hash = get_prefix_for_item(item);

   //printf("Inserting %lu version %lu\n", get_prefix_for_item(item), get_rdt_value(e));
   btree_insert(items_locations[worker_id], (unsigned char*)&hash, sizeof(hash), e);
}

void memory_index_delete(int worker_id, void *item) {
   assert(is_worker_context());

   index_entry_t *old_entry = NULL;
   uint64_t hash = get_prefix_for_item(item);

   btree_delete(items_locations[worker_id], (unsigned char *)&(hash), sizeof(hash));

   if(old_entry)
      free(old_entry);
}


/* Item has been written to disk, and is THEN added in the index */
void memory_index_add(struct slab_callback *cb, void *item) {
   index_entry_t new_entry;
   struct item_metadata *meta = item;
   new_entry.slab = cb->slab;
   new_entry.slab_idx = cb->slab_idx;
   new_entry.rdt = meta->rdt;
   assert(new_entry.rdt);
   memory_index_insert(get_worker(new_entry.slab), item, &new_entry);
}

/* Item has NOT been written to disk yet, but we want to say that we will in order to avoid write conflicts */
void memory_index_reserve(int worker_id, void *item, uint64_t transaction_id) {
   index_entry_t new_entry;
   new_entry.slab = NULL;
   new_entry.slab_idx = 0;
   new_entry.rdt = transaction_id;
   set_locked_bit(&new_entry);
   memory_index_insert(worker_id, item, &new_entry);
}



size_t get_snapshot_size(void) {
   size_t total = 0;
   for(size_t i = 0; i < get_nb_workers(); i++)
      total += nb_snapshotted_items[i];
   return total;
}

void memory_index_init(void) {
   items_locations = malloc(get_nb_workers() * sizeof(*items_locations));
   old_items_locations = malloc(get_nb_workers() * sizeof(*old_items_locations));
   nb_snapshotted_items = calloc(get_nb_workers(), sizeof(*nb_snapshotted_items));
   for(size_t w = 0; w < get_nb_workers() ; w++) {
      items_locations[w] = btree_create();
      old_items_locations[w] = btree_create();
   }
}


