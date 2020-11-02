#include "headers.h"

#define FLAG_READ 1
#define FLAG_WRITE 2

// TODO: prevent user to call more reads / write on an aborted transaction

struct transaction {
   size_t transaction_id;
   size_t transaction_id_on_disk;
   size_t snapshot;
   size_t size;
   size_t used_size;
   char *cached_data;
   btree_t *index;
   int failed;
   size_t nb_items;
   size_t nb_items_written_on_disk;
   size_t nb_items_fully_committed;
   int complete;


   int in_commit;
   int has_write;
   volatile struct slab_callback **delayed_work;
   void *payload;
   uint64_t rdt_start;
   struct slab_callback *map;
   struct injector_queue *injector_queue;
   struct transaction *long_prev, *long_next;
};

size_t get_unique_key_for_transaction(struct transaction *t) {
   return -10 - t->transaction_id_on_disk; // The -10 is here to avoid some conflict in key naming... It is a bit hacky -- ideally we would store transactions in their own tree instead of storing them in the regular index were they could conflict with other items...
}

size_t get_transaction_id(struct transaction *t) {
   if(!t)
      return -1;
   return t->transaction_id;
}

void set_transaction_id(struct transaction *t, uint64_t v) {
   t->transaction_id = v;
}

size_t get_transaction_id_on_disk(struct transaction *t) {
   return t->transaction_id_on_disk;
}

uint64_t get_snapshot_version(struct transaction *t) {
   if(!t)
      return -1;
   return t->snapshot;
}

void set_snapshot_version(struct transaction *t, uint64_t v) {
   t->snapshot = v;
}

int has_failed(struct transaction *t) {
   return t->failed;
}

void set_payload(struct transaction *t, void *data) {
   t->payload = data;
}

void *get_payload(struct transaction *t) {
   return t->payload;
}

uint64_t get_start_time(struct transaction *t) {
   return t->rdt_start;
}

int is_complete(struct transaction *t) {
   return t->complete;
}

int is_in_commit(struct transaction *t) {
   return t->in_commit;
}

struct slab_callback *get_map(struct transaction *t) {
   return t->map;
}

struct transaction *transaction_get_long_next(struct transaction *t) {
   return t->long_next;
}

struct transaction *transaction_get_long_prev(struct transaction *t) {
   return t->long_prev;
}

void transaction_set_long_next(struct transaction *t, struct transaction *next) {
   t->long_next = next;
}

void transaction_set_long_prev(struct transaction *t, struct transaction *prev) {
   t->long_prev = prev;
}


static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

int set_abort_flag(struct transaction *t, struct slab_callback *trans_callback) {
   if(trans_callback->failed) { // operation aborted by the KV because of conflicting transaction => abort
      t->failed = 1;
   }
   return t->failed;
}

/*
 * Transaction rules on what can or cannot be done on an item
 */
int action_allowed(index_entry_t *e, struct slab_callback *cb) {
   if(!e)
      return 1;

   enum slab_action action = cb->action;
   size_t transaction_id = get_transaction_id(cb->transaction);

   if(transaction_id == -1) {          // Not inside a transaction
      if(action == READ) {             // Always allowed to read...
         return e->slab != NULL;       // ... except if the item does not exist (spot reserved by a transaction but not yet in the DB)
      } else if(get_locked_bit(e)) {   // We can also always write, except if the item is protected by a lock
         return 0;
      }
      return 1;
   }

   if(action == START_TRANSACTION_COMMIT) // special case for transaction markers -- discard timestamps
      return 1;

   size_t snapshot = cb->transaction->snapshot;
   /* Inside a transaction, we want to avoid anything we couldn't have read from a snapshot */
   if(action == END_TRANSACTION_COMMIT) {
      assert(get_rdt_value(e) == cb->transaction->transaction_id_on_disk); // only a given transaction is allowed to end itself...
      return 1;
   } else if(action == READ || action == READ_NEXT || action == READ_NEXT_BATCH) {
      if(get_rdt_value(e) > snapshot) { // Item is too recent ==> abort
         //printf("Trans %lu Refusing to read %lu because %lu > %lu\n", transaction_id, get_prefix_for_item(cb->item), get_rdt_value(e), snapshot);
         return 0;
      }
      if(e->slab == NULL) { // Item has been reserved but has not been written yet ==> abort
         //printf("Refusing to read %lu because slab is NULL\n", get_prefix_for_item(cb->item));
         return 0;
      }
      return 1;
   } else {
      if(get_locked_bit(e)) { // Item is locked ==> abort
         //printf("Not allowed because of lock, action %d, item %lu\n", action, get_prefix_for_item(cb->item));
         return 0;
      }
      if(get_rdt_value(e) > snapshot) { // Item is too recent ==> abort
         //printf("Not allowed because of snapshot %lu vs %lu, action %d, item %lu\n", get_rdt_value(e), snapshot, action, get_prefix_for_item(cb->item));
         return 0;
      }
      return 1;
   }
}

/*
 * Is an item in the transaction cache?
 */
static index_entry_t *transaction_lookup(struct transaction *t, void *item) {
   if(!item)
      return NULL;

   index_entry_t *tmp_entry;
   uint64_t hash = get_prefix_for_item(item);
   int res = btree_find(t->index, (unsigned char*)&hash, sizeof(hash), &tmp_entry);
   if(res)
      return tmp_entry;
   else
      return NULL;
}

static void* transaction_cached_get(struct transaction *t, struct slab_callback *callback) {
   index_entry_t *e = transaction_lookup(t, callback->item);
   if(!e)
      return NULL;
   else
      return &t->cached_data[e->cached_data_idx];
}

/*
 * Put an item in the transaction cache
 */
static void transaction_cached_put(struct transaction *t, void *item, size_t flags) {
   if(!item)
      return;

   uint64_t hash = get_prefix_for_item(item);
   uint64_t item_size = get_item_size(item);
   index_entry_t *e = transaction_lookup(t, item);
   if(e) { // data is already cached in
      uint64_t old_size = get_item_size(&t->cached_data[e->cached_data_idx]);
      flags |= e->transaction_flags;
      if(old_size != item_size || flags != e->transaction_flags) {
         btree_delete(t->index, (unsigned char*)&hash, sizeof(hash));
      } else {
         memcpy(&t->cached_data[e->cached_data_idx], item, item_size);
         return;
      }
   } else {
      t->nb_items++;
   }

   // Data is not cached (or size changed or flags changed)
   index_entry_t tmp;
   tmp.cached_data_idx = t->used_size;
   t->used_size += item_size;
   assert(t->used_size < t->size); //TODO: resize!
   tmp.transaction_flags = flags;
   memcpy(&t->cached_data[tmp.cached_data_idx], item, item_size);
   btree_insert(t->index, (unsigned char*)&hash, sizeof(hash), &tmp);
}


/*
 * API
 */
static void complete_client_op(struct slab_callback *trans_callback, void *item) {
   struct slab_callback *callback = trans_callback->payload;
   callback->action = trans_callback->action;
   callback->next = NULL;
   if(callback->cb)
      callback->cb(callback, item); // direct call because always in good context
   free(trans_callback);
}

/*
 * READ = get from the main DB
 */
void kv_trans_read_cb(struct slab_callback *trans_callback, void *item) {
   struct transaction *t = trans_callback->transaction;
   set_abort_flag(t, trans_callback);
   complete_client_op(trans_callback, item);
}

void kv_trans_read(struct transaction *t, struct slab_callback *callback) {
   assert(callback->injector_queue); // Transactions are not thread safe and callbacks must be called within a thread context
   callback->transaction = t;
   callback->action = READ;

   if(has_failed(t)) {
      if(callback->cb)
         callback->cb(callback, NULL);
      return;
   }

   void *data = transaction_cached_get(t, callback);
   if(data) {
      if(callback->cb)
         callback->cb(callback, data);
      return;
   }

   struct slab_callback *new_cb = calloc(1, sizeof(*new_cb));
   new_cb->transaction = t;
   new_cb->cb = kv_trans_read_cb;
   new_cb->payload = callback;
   new_cb->item = callback->item;
   new_cb->injector_queue = callback->injector_queue;
   kv_read_async(new_cb);
}

/*
 * Write
 */
void kv_trans_write_cb(struct slab_callback *trans_callback, void *item) {
   struct transaction *t = trans_callback->transaction;
   assert(t->cached_data);
   set_abort_flag(t, trans_callback);
   if(!trans_callback->failed) // we check on the *callback* not on the transaction; the transaction might have failed due to a previous callback but *this* callback might have succeeded in locking the item
      transaction_cached_put(trans_callback->transaction, trans_callback->item, FLAG_WRITE); // if it succeeded, the item *must* be placed in the btree so that we unlock it when committing or aborting, otherwise the lock is never released
   complete_client_op(trans_callback, item);
}


void kv_trans_write(struct transaction *t, struct slab_callback *callback) {
   assert(t->cached_data);
   assert(callback->item);
   assert(callback->injector_queue); // Transactions are not thread safe and callbacks must be called within a thread context
   assert(t->cached_data);
   callback->transaction = t;
   callback->action = READ_FOR_WRITE;

   if(has_failed(t)) {
      if(callback->cb)
         callback->cb(callback, NULL);
      return;
   }

   t->has_write = 1;

   index_entry_t *e = transaction_lookup(t, callback->item);
   if(e) { // data is cached
      if(e->transaction_flags & FLAG_WRITE) { // second update to the same item, ok, do the update in the cache (no need to propagate)
         transaction_cached_put(t, callback->item, FLAG_WRITE);
         if(callback->cb)
            callback->cb(callback, NULL);
         return;
      }
   }

   // Otherwise we need to lock the item in the in memory index, so call the main DB for that
   struct slab_callback *new_cb = calloc(1, sizeof(*new_cb));
   new_cb->transaction = t;
   new_cb->cb = kv_trans_write_cb;
   new_cb->payload = callback;
   new_cb->item = callback->item;
   new_cb->injector_queue = callback->injector_queue;
   kv_lock_async(new_cb);
}

/*
 * Commit
 */

/*
 * 4/ Slow path -- All the items have written, old values have been deleted, time to end the commit
 * Called in injector context.
 */
void kv_commit_cb(struct slab_callback *trans_callback, void *item) {
   struct transaction *t = trans_callback->transaction;
   register_end_transaction(t->transaction_id, t->transaction_id_on_disk);

   btree_free(t->index);
   free(t->delayed_work);
   free(t->cached_data);

   struct slab_callback *user_callback = trans_callback->payload;
   if(user_callback->cb)
      user_callback->cb(user_callback, NULL);
   free(trans_callback->item);
   free(trans_callback);
}

/*
 * 3/ Slow path -- Called every time a new value has been written.
 * When all have been written, remove transaction ID from the transaction slab.
 * Called in injector context
 */
void write_items_to_disk_cb(struct slab_callback *callback, void *item) {
   struct transaction *t = callback->transaction;
   size_t done = __sync_fetch_and_add(&t->nb_items_written_on_disk, 1);
   if(done == t->nb_items - 1) {
      struct slab_callback *cb = new_slab_callback();
      cb->transaction = t;
      cb->payload = callback->payload;
      cb->injector_queue = callback->injector_queue;
      cb->item = create_unique_item_with_value(TRANSACTION_OBJECT_SIZE, get_unique_key_for_transaction(t), t->nb_items);
      cb->cb = kv_commit_cb;
      kv_end_commit(cb); // TODO: could be optimized, we don't actually need to remove the value on disk
                        //    because we have the nb items as values ==> could use that in recovery
   }
   // do not free callback->item because it refers to cached data
   free(callback);
}


/*
 * 2/ Slow path -- For each key in the transaction index, write the new value to disk
 * If the transaction crashes mid-way, these values will be ignored.
 * Items are still locked in the indexes
 * Called in injector context.
 */
void write_items_to_disk(uint64_t hash, void *data) {
   void *cached_data;
   index_entry_t *tmp_entry;
   struct slab_callback *callback = data;
   struct transaction *t = callback->transaction;

   int res = btree_find(t->index, (unsigned char*)&hash, sizeof(hash), &tmp_entry);
   assert(res);
   cached_data = &t->cached_data[tmp_entry->cached_data_idx];

   if(tmp_entry->transaction_flags & FLAG_WRITE) {
      struct slab_callback *new_cb = calloc(1, sizeof(*new_cb));
      new_cb->transaction = t;
      new_cb->cb = write_items_to_disk_cb;
      new_cb->payload = callback;
      new_cb->injector_queue = callback->injector_queue;
      new_cb->item = cached_data;
      kv_update_async(new_cb); // has to be done by injector!
   } else { // read = no need to propagate, we are done!
      assert(0);
   }
}

/*
 * 1/ Slow path -- Transaction ID has been written in the transaction slab, now iterate over all keys to find out which ones to write
 * Called injector_queue context. User callback is in trans_callback->payload.
 */
void kv_start_commit_cb(struct slab_callback *trans_callback, void *item) {
   struct transaction *t = trans_callback->transaction;
   btree_forall_keys(t->index, write_items_to_disk, trans_callback->payload);
   free(trans_callback->item);
   free(trans_callback);
}

/*
 * Commit fast path -- either an abort or a read only transaction
 */

/*
 * 4/ Helper function to end the transaction
 * Called in injector context
 */
void kv_end_commit_fast_path(struct transaction *t, struct slab_callback *user_callback) {
   register_end_transaction(t->transaction_id, t->transaction_id_on_disk);
   btree_free(t->index);
   free(t->delayed_work);
   free(t->cached_data);
   t->cached_data = NULL;
   if(user_callback->cb)
      user_callback->cb(user_callback, NULL);
}

/*
 * 3/ Fast path -- all locks released, time to end
 * Called in injector context
 */
void kv_commit_fast_path_cb(struct slab_callback *trans_callback, void *item) {
   struct transaction *t = trans_callback->transaction;
   size_t done = __sync_fetch_and_add(&t->nb_items_fully_committed, 1);
   if(done == t->nb_items - 1) {
      kv_end_commit_fast_path(t, trans_callback->payload);
   }
   free(trans_callback);
}

/*
 * 2/ Fast path -- ask the DB to release all the locks taken by the transaction
 * Called in injector context
 */
void release_transaction_locks(uint64_t hash, void *data) {
   void *cached_data;
   index_entry_t *tmp_entry;
   struct slab_callback *callback = data;
   struct transaction *t = callback->transaction;

   int res = btree_find(t->index, (unsigned char*)&hash, sizeof(hash), &tmp_entry);
   assert(res);
   cached_data = &t->cached_data[tmp_entry->cached_data_idx];

   if(tmp_entry->transaction_flags & FLAG_WRITE) {
      struct slab_callback *new_cb = calloc(1, sizeof(*new_cb));
      new_cb->transaction = t;
      new_cb->cb = kv_commit_fast_path_cb;
      new_cb->payload = callback;
      new_cb->item = cached_data;
      new_cb->injector_queue = callback->injector_queue;
      kv_revert_update(new_cb);
   } else { // read = no need to propagate, we are done!
      assert(0); // we don't cache reads anymore!
   }
}

/*
 * 1/ Fast path -- iterate on the index
 * Called in injector context
 */
void commit_fast_path(struct transaction *t, struct slab_callback *callback) {
   if(t->has_write && t->nb_items) {
      btree_forall_keys(t->index, release_transaction_locks, callback);
   } else {
      kv_end_commit_fast_path(t, callback);
   }
}

/*
 * 0/ Start of the commit
 */
int kv_commit(struct transaction *t, struct slab_callback *callback) {
   t->in_commit = 1;
   register_start_commit(t);
   callback->transaction = t;

   //printf("Committing %lu (failed %d) size %lu\n", t->transaction_id, t->failed, t->nb_items);

   if(t->failed) {
      commit_fast_path(t, callback);
      return 1;
   }
   if(!t->has_write) { // nothing to do!
      commit_fast_path(t, callback);
      return 0;
   }
   assert(callback->injector_queue); // Transactions are not thread safe and callbacks must be called within a thread context

   /* From this point on, we know that the commit cannot abort, but it can crash! */
   t->transaction_id_on_disk = register_commit_transaction(t->transaction_id);

   /* First, write the transaction ID in the transaction slab, so that we can ignore it if it crashes mid-way */
   struct slab_callback *cb = new_slab_callback();
   cb->transaction = t;
   cb->item = create_unique_item_with_value(TRANSACTION_OBJECT_SIZE, get_unique_key_for_transaction(t), t->nb_items);
   cb->payload = callback;
   cb->cb = kv_start_commit_cb;
   cb->injector_queue = callback->injector_queue;
   kv_start_commit(cb);

   return 0;
}

int kv_abort(struct transaction *t, struct slab_callback *callback) {
   t->failed = 1;
   kv_commit(t, callback);
   return 0;
}

/*
 * Create a transaction object
 */
struct transaction *create_generic_transaction(struct slab_callback *map, void *payload) {
   struct transaction *t = calloc(1, sizeof(*t));
   t->map = map;
   t->payload = payload;
   if(map) {
      map->action = MAP;
      map->transaction = t;
   }

   register_new_transaction(t);
   size_t size = 2048*PAGE_SIZE;
   t->size = size;
   t->cached_data = malloc(t->size);
   if(!t->cached_data) {
      system("free");
      die("Cannot allocate cached data!! Asked %lu %lu\n", t->size, size);
   }
   assert(t->cached_data);
   t->index = btree_create();
   rdtscll(t->rdt_start);
   return t;
}

/*
 * Short transaction
 */
struct transaction *create_transaction(void) {
   return create_generic_transaction(NULL, NULL);
}

/*
 * Support for long running transactions
 */
static void scan_cb(struct slab_callback *cb, void *item);
struct transaction *create_long_transaction(struct slab_callback *map, void *payload) {
   return create_generic_transaction(map, payload);
}

void transaction_propagate(void *item, uint64_t max_snapshot_id) {
   forall_long_running_transaction(item_get_rdt(item), max_snapshot_id, item);
}

/*
 * Interface for Map Reduce scans!
 */
struct scan_payload {
   size_t completed_workers;
   size_t start, end;
   slab_cb_t *map;
   struct per_worker_data {
      uint64_t end_last_batch;
      uint64_t end_current_batch;
      size_t current_batch_size;
      size_t *current_batch_hashes;
      size_t current_batch_seen;
      size_t *seen_batch_hashes;
   } *per_worker;
   //int *seen;
};

void register_next_batch(struct slab_callback *cb, size_t size, size_t *hashes) {
   struct scan_payload *p = get_payload(cb->transaction);
   struct per_worker_data *w = &p->per_worker[get_worker_id()];
   if(w->current_batch_size) {
      assert(w->current_batch_seen == w->current_batch_size); // can only do next batch if previous is entirely processed!
      w->end_last_batch = w->current_batch_hashes[w->current_batch_size - 1];
      free(w->current_batch_hashes);
   }
   assert(size <= MAX_BATCH_SIZE);
   w->end_current_batch = hashes[size - 1];
   w->current_batch_size = size;
   w->current_batch_hashes = hashes;
   w->current_batch_seen = 0;
   memset(w->seen_batch_hashes, 0, size*sizeof(*w->seen_batch_hashes));
}

static int is_processable(struct scan_payload *p, int worker, void *item) {
   struct per_worker_data *w = &p->per_worker[worker];
   uint64_t key = item_get_key(item);
   if(key <= w->end_last_batch || key >= p->end)
      return 0;
   if(key > w->end_current_batch)
      return 2;
   for(size_t i = 0; i < w->current_batch_size; i++) {
      if(key == w->current_batch_hashes[i]) {
         if(w->seen_batch_hashes[i])
            return 0;
         w->seen_batch_hashes[i] = 1;
         w->current_batch_seen++;
         return 1;
      } else if(key < w->current_batch_hashes[i]) {
         return 2;
      }
   }
   die("Bug");
}

static int is_batch_complete(struct scan_payload *p, int worker) {
   return p->per_worker[worker].current_batch_seen == p->per_worker[worker].current_batch_size;
}

static void scan_next(struct slab_callback *cb, void *item) {
   if(is_worker_context()) {
      int worker = get_worker_id();
      struct scan_payload *p = get_payload(cb->transaction);
      uint64_t last_hash = p->per_worker[worker].current_batch_hashes[ p->per_worker[worker].current_batch_size - 1 ];
      struct slab_callback *ncb = new_slab_callback();
      ncb->cb = scan_next;
      //cb->injector_queue = q;
      ncb->payload = cb->payload;
      ncb->item = create_key(last_hash);
      ncb->max_next_key = cb->max_next_key;
      ncb->transaction = cb->transaction;
      ncb->injector_queue = cb->transaction->injector_queue;
      injector_insert_in_queue(get_worker_id(), ncb->injector_queue, ncb, NULL);
   } else {
      cb->cb = scan_cb;
      cb->injector_queue = NULL;
      kv_read_next_batch_async(cb, get_worker_for_item(cb->item));
   }
}

static void scan_commit_cb2(struct slab_callback *cb, void *item) {
   struct scan_payload *p = get_payload(cb->transaction);
   for(size_t i = 0; i < get_nb_workers(); i++) {
      free(p->per_worker[i].current_batch_hashes);
      free(p->per_worker[i].seen_batch_hashes);
   }
   free(p->per_worker);
   //free(p->seen);
   free(p);
   free(cb);
}

static void scan_commit_cb1(struct slab_callback *cb, void *item) {
   cb->cb = scan_commit_cb2;
   kv_commit(cb->transaction, cb);
}

static void scan_map(struct slab_callback *cb, void *item) {
   struct scan_payload *p = get_payload(cb->transaction);
   //uint64_t key = item_get_key(item);
   assert(cb->action == MAP);

   if(is_worker_context()) { // Worker asks us if we want to handle the item?
      int can_process = is_processable(p, get_worker_id(), item);
      if(!can_process) {
         //printf("IGNORE Key %lu RDT %lu (map trans %lu )\n", item_get_key(item), item_get_rdt(item), get_transaction_id(cb->transaction));
         //if(!p->seen[key])
         //printf("Trans %lu race on key %lu\n", get_transaction_id(cb->transaction), key);
      } else { // propagate key
         //printf("PROPAGATE Key %lu RDT %lu (map trans %lu )\n", item_get_key(item), item_get_rdt(item), get_transaction_id(cb->transaction));
         p->map(cb->payload, item);
         //assert(item_get_key(item) < p->end);
         //p->seen[item_get_key(item)]++;
         if(can_process == 1 && is_batch_complete(p, get_worker_id())) // if can_process == 2, the scanned item is not part of the current batch
            scan_next(cb, item);
      }
   } else { // the key is within our scanned range!
      die("Map called from non worker context?!\n");
   }
}

static void scan_cb(struct slab_callback *cb, void *item) {
   struct scan_payload *p = get_payload(cb->transaction);
   if(cb->raced) {
      //if(!p->seen[cb->next_key])
      //printf("Trans %lu race ter on key %lu\n", get_transaction_id(cb->transaction), cb->next_key);
   } else {
      if(!item) { // end of the scan
         int completed = __sync_add_and_fetch(&p->completed_workers, 1);
         if(completed == get_nb_workers()) { // Time to commit
            //for(size_t i = 1; i < (p->end - p->start); i++) {
            //if(p->seen[i] != 1) {
            //printf("Trans %lu Race not/too seen %lu - %d\n", get_transaction_id(cb->transaction), i, p->seen[i]);
            //}
            //}

            p->map(cb->payload, NULL);

            struct slab_callback *new_cb = new_slab_callback();
            new_cb->cb = scan_commit_cb1;
            new_cb->transaction = cb->transaction;
            new_cb->injector_queue = cb->transaction->injector_queue;
            injector_insert_in_queue(get_worker_id(), new_cb->injector_queue, new_cb, NULL);
         }
      } else if(!is_processable(p, get_worker_id(), item)) { // !!! is_processable must be called, it is not pure!!
         die("Bug");
      } else {
         p->map(cb->payload, item);
         assert(item_get_key(item) < p->end);
         //p->seen[item_get_key(item)]++;
         if(is_batch_complete(p, get_worker_id()))
            scan_next(cb, cb->item);
      }
   }
   free(cb->item);
   free(cb);
}

void kv_long_scan(struct slab_callback *_cb) {
   struct scan_payload *p = calloc(1, sizeof(*p));
   p->per_worker = calloc(get_nb_workers(), sizeof(*p->per_worker));
   for(size_t i = 0; i < get_nb_workers(); i++) {
      p->per_worker[i].seen_batch_hashes = calloc(MAX_BATCH_SIZE, sizeof(*p->per_worker[i].seen_batch_hashes));
   }
   p->end = _cb->max_next_key;
   p->map = _cb->cb;
   //p->seen = calloc(_cb->max_next_key - item_get_key(_cb->item), sizeof(*p->seen));

   /* Create the long transaction scan */
   struct transaction *t;
   struct slab_callback *mcb = new_slab_callback();
   mcb->cb = scan_map;
   mcb->payload = _cb;
   mcb->max_next_key = _cb->max_next_key;
   mcb->injector_queue = _cb->injector_queue;
   t = create_long_transaction(mcb, p);
   t->injector_queue = _cb->injector_queue;

   /* Start the scan */
   struct slab_callback *cb = new_slab_callback();
   cb->cb = scan_cb;
   cb->payload = _cb;
   //cb->injector_queue = q;
   cb->item = clone_item(_cb->item);
   cb->max_next_key = _cb->max_next_key;
   cb->transaction = t;
   for(int w = 1; w < get_nb_workers(); w++) {
      struct slab_callback *ncb = clone_callback(cb);
      ncb->item = clone_item(cb->item);
      kv_read_next_batch_async(ncb, w);
   }
   kv_read_next_batch_async(cb, 0);
}


