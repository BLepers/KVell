#include "headers.h"

/*
 * A slab worker takes care of processing requests sent to the KV-Store.
 * E.g.:
 *    kv_add_async(...) results in a request being enqueued (enqueue_slab_callback function) into a slab worker
 *    The worker then dequeues the request, calls functions of slab.c to figure out where the item is on disk (or where it should be placed).
 *
 * Because we use async IO, the worker can enqueue/dequeue more callbacks while IOs are done by the drive (complete_processed_io(...)).
 *
 * A slab worker has its own slab, no other thread should touch the slab. This is straightforward in the current design: a worker sends IO requests
 * for its slabs and processes answers for its slab only.
 *
 * We have the following files on disk:
 *  If we have W disk workers per disk
 *  If we have S slab workers
 *  And Y disks
 *  Then we have W * S * Y files for any given item size:.
 *  /scratchY/slab-a-w-x = slab worker a, disk worker w, item size x on disk Y
 *
 * The slab.c functions abstract many disks into one, so
 *   /scratch** /slab-a-*-x  is the same virtual file
 * but it is a different slab from
 *   /scratch** /slab-b-*-x
 * To find in which slab to insert an element (i.e., which slab worker to use), we use the get_slab function bellow.
 */

static __thread int worker_context = 0;
static __thread int worker_id = 0;
static int nb_workers = 0;
static int nb_disks = 0;
static int nb_workers_launched = 0;
static int nb_workers_ready = 0;

int get_nb_workers(void) {
   return nb_workers;
}

int get_nb_disks(void) {
   return nb_disks;
}

int get_worker_id() {
   assert(worker_context);
   return worker_id;
}



/*
 * Worker context - Each worker thread in KVell has one of these structure
 */
size_t slab_sizes[] = { 100, 128, 256, 400, 512, 1024, 1365, 2048, 4096 };
struct cb_queue {                                        // A queue of requests pending to be processed by a worker
   pthread_mutex_t lock;
   pthread_cond_t cond;
   struct slab_callback *head;
   struct slab_callback *tail;
   volatile size_t nb_callbacks;
   size_t nb_total_processed_callbacks;
   size_t max_pending_callbacks;                         // Maximum possible number of enqueued requests
};
struct slab_context {
   size_t worker_id __attribute__((aligned(64)));        // ID
   struct slab **slabs;                                  // Files managed by this worker
   struct slab *transactions_slab;                       // File used to store transactions

   struct cb_queue cb_queue;                             // Regular queued requests

   struct pagecache *pagecache __attribute__((aligned(64)));
   struct io_context *io_ctx;
   struct to_be_freed_list *gc;
   uint64_t rdt;                                         // Latest timestamp
   int idle;
} *slab_contexts;
static pthread_barrier_t slab_barrier;                   // Will unblock when all workers are done with recovering their files

/* A file is only managed by 1 worker. File => worker function. */
int get_worker(struct slab *s) {
   return s->ctx->worker_id;
}

struct pagecache *get_pagecache(struct slab_context *ctx) {
   return ctx->pagecache;
}

struct io_context *get_io_context(struct slab_context *ctx) {
   return ctx->io_ctx;
}

uint64_t get_rdt(struct slab_context *ctx) {
   return ctx->rdt;
}

void set_rdt(struct slab_context *ctx, uint64_t val) {
   ctx->rdt = val;
}


/*
 * The DB maintains a global timestamp that corresponds to the latest items written to disk.
 */
static pthread_mutex_t biggest_rdt_lock;
static uint64_t biggest_rdt = 0;
void set_highest_rdt(uint64_t val) {
   pthread_mutex_lock(&biggest_rdt_lock);
   if(val > biggest_rdt)
      biggest_rdt = val;
   pthread_mutex_unlock(&biggest_rdt_lock);
}

uint64_t get_highest_rdt(void) {
   return __sync_add_and_fetch(&biggest_rdt, 1);
}


/*
 * Transaction context.
 * It is possible that a transaction is only partially committed (crash). This structure remembers which transactions to ignore during recovery.
 */
static pthread_barrier_t transaction_barrier;            // Will unblock when all workers have read their file of partially committed transactions
static pthread_mutex_t transaction_recovery_context_lock;
struct transaction_recovery_context {
   uint64_t *ignored_rdts;                               // Partially committed transactions to ignore during recovery
   uint64_t nb_ignored_rdts;                             // ...
} transaction_recovery_context;


/*
 * Injector context
 */
void check_races_and_call(struct slab_callback *callback, void *item) {
   if(TRANSACTION_TYPE == TRANS_LONG && item) {
      int race = 0;
      switch(callback->action) {
         case READ:
            if(item_get_key(item) != item_get_key(callback->item)
                  || (callback->transaction && get_snapshot_version(callback->transaction) < item_get_rdt(item)))
               race = 1;
            break;
         case READ_NEXT:
         case READ_NEXT_BATCH:
         case READ_NEXT_BATCH_CLONE:
            if(item_get_key(item) != callback->next_key
                  || (callback->transaction && get_snapshot_version(callback->transaction) < item_get_rdt(item)))
               race = 1;
            break;
         default:
            break;
      }
      if(race) {
         // Unlikely data race: we were reading a spot that was in the freelist (possible in TRANS_LONG), and the spot was reused
         // before we got a chance to actually read it! No big deal, the old value has been propagated, so just inform the callback
         // of the error. Print is just here for debug.
         //printf("#WARNING: unlikely data race, let's retry! Keys %lu vs %lu or %lu, rdt %lu vs %lu\n", item_get_key(item), item_get_key(callback->item), callback->next_key, get_snapshot_version(callback->transaction), item_get_rdt(item));
         callback->raced = 1;
         callback->cb(callback, NULL);
         return;
      }
   }
   callback->cb(callback, item);
}

void call_callback(struct slab_callback *callback, void *item) {
   //static __thread declare_periodic_overhead;
   if(!callback->cb)
      return;

   //start_periodic_overhead {
   if(!callback->injector_queue)
      check_races_and_call(callback, item);
   else
      injector_insert_in_queue(get_worker_id(), callback->injector_queue, callback, item);
   //} stop_periodic_overhead(1000, "WORKER BREAKDOWN", "call_callback");
}

/*
 * When a request is submitted by a user, it is enqueued. Functions to do that.
 */

/* How many callbacks are enqueued in a given queue? */
static size_t get_nb_pending_callbacks(struct cb_queue *q) {
   return q->nb_callbacks;
}

/* We shard data based on the prefix of the key */
static uint64_t get_hash_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* Requests are statically attributed to workers using this function */
static struct slab_context *get_slab_context(void *item) {
   uint64_t hash = get_hash_for_item(item);
   return &slab_contexts[hash%get_nb_workers()];
}

static struct slab_context *get_slab_context_from_hash(uint64_t hash) {
   return &slab_contexts[hash%get_nb_workers()];
}

struct to_be_freed_list *get_gc_for_item(char *item) {
   return get_slab_context(item)->gc;
}

static struct slab *get_slab(struct slab_context *ctx, void *item) {
   size_t item_size = get_item_size(item);
   for(size_t i = 0; i < sizeof(slab_sizes)/sizeof(*slab_sizes); i++) {
      if(item_size <= slab_sizes[i])
         return ctx->slabs[i];
   }
   die("Item is too big\n");
}

struct slab *get_item_slab(void *item) {
   struct slab_context *ctx = get_slab_context(item);
   return get_slab(ctx, item);
}

/* Called by the main thread when waiting for requests */
static void wait_for_requests(struct cb_queue *q) {
   pthread_mutex_lock(&q->lock);
   volatile size_t pending = get_nb_pending_callbacks(q);
   if(pending) {
      pthread_mutex_unlock(&q->lock);
   } else {
      pthread_cond_wait(&q->cond, &q->lock);
      pthread_mutex_unlock(&q->lock);
   }
}

/* Called by the injector to make sure that the request queue is not full */
static void wait_for_free_spot_keep_lock(struct cb_queue *q) {
again:
   if(!PINNING || !SPINNING) { // no active waiting
      pthread_mutex_lock(&q->lock);
      if(q->nb_callbacks >= q->max_pending_callbacks) {
         pthread_cond_wait(&q->cond, &q->lock); // queue is full, wait
      }
   } else {
      while(q->nb_callbacks >= q->max_pending_callbacks) {
         NOP10();
      }
      pthread_mutex_lock(&q->lock);
   }

   if(q->nb_callbacks >= q->max_pending_callbacks) {
      pthread_mutex_unlock(&q->lock);
      goto again;
   }
}

static void enqueue_slab_callback(struct slab_context *ctx, enum slab_action action, struct slab_callback *callback) {
   if(is_worker_context())
      die("Trying to perform kv_... operations from a callback is forbidden without using injector contexts\n");

   static __thread declare_periodic_overhead;
   struct cb_queue *q = &ctx->cb_queue;
   callback->action = action;
   add_time_in_payload(callback, 0);

   start_periodic_overhead {
      wait_for_free_spot_keep_lock(q); // takes q->lock
   } stop_periodic_overhead(1000, "INJECTOR BREAKDOWN", "request_queue_full");

   callback->next = NULL;
   add_time_in_payload(callback, 1);

   if(!q->head) {
      q->head = callback;
      q->tail = callback;
   } else {
      q->tail->next = callback;
      q->tail = callback;
   }

   if(q->nb_callbacks == 0)
      pthread_cond_broadcast(&q->cond); // signal the main thread that it now has work, wakes up wait_for_requests()

   q->nb_callbacks++;
   pthread_mutex_unlock(&q->lock);
}

/*
 * KVell API - These functions are called from user context
 */
int get_worker_for_item(void *item) {
   struct slab_context *ctx = get_slab_context(item);
   return ctx->worker_id;
}

int get_worker_for_prefix(uint64_t hash) {
   struct slab_context *ctx = get_slab_context_from_hash(hash);
   return ctx->worker_id;
}

void *kv_read_sync(void *item) {
   struct slab_context *ctx = get_slab_context(item);
   index_entry_t *e = memory_index_lookup(ctx->worker_id, NULL, item, -1, NULL);
   if(e)
      return read_item(e->slab, e->slab_idx);
   else
      return NULL;
}

void kv_read_sync_safe_cb(struct slab_callback *cb, void *item) {
   cb->payload = item;
}
void *kv_read_sync_safe(void *item) {
   if(is_worker_context())
      return kv_read_sync(item);

   struct injector_queue *q = create_new_injector_queue();
   struct slab_callback *cb = new_slab_callback();
   cb->item = item;
   cb->injector_queue = q;
   cb->cb = kv_read_sync_safe_cb;
   kv_read_async(cb);

   do {
      injector_process_queue(q);
   } while(pending_work() || injector_has_pending_work(q));

   void *ret = cb->payload;
   free(cb);
   free(q);
   return ret;
}

void kv_read_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, READ, callback);
}

void kv_read_for_write_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, READ_FOR_WRITE, callback);
}

void kv_lock_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, LOCK, callback);
}

void kv_revert_update(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, REVERT, callback);
}

void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx) {
   callback->slab = s;
   callback->slab_idx = slab_idx;
   return enqueue_slab_callback(s->ctx, READ_NO_LOOKUP, callback);
}

void kv_add_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   enqueue_slab_callback(ctx, ADD, callback);
}

void kv_update_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, UPDATE, callback);
}

void kv_update_in_place_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, UPDATE_IN_PLACE, callback);
}

void kv_add_or_update_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, ADD_OR_UPDATE_IN_PLACE, callback);
}

void kv_remove_async(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, DELETE, callback);
}

void kv_start_commit(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, START_TRANSACTION_COMMIT, callback);
}

void kv_end_commit(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, END_TRANSACTION_COMMIT, callback);
}

void kv_read_next_async(struct slab_callback *callback, int worker) {
   struct slab_context *ctx = &slab_contexts[worker];
   return enqueue_slab_callback(ctx, READ_NEXT, callback);
}

void kv_read_next_batch_async(struct slab_callback *callback, int worker) {
   struct slab_context *ctx = &slab_contexts[worker];
   return enqueue_slab_callback(ctx, READ_NEXT_BATCH, callback);
}

/*
 * Worker context
 */

/* Dequeue enqueued callbacks */
static int worker_do_one_request(struct slab_context *ctx, struct slab_callback *callback, int max_extra_io) {
   add_time_in_payload(callback, 2);

   index_entry_t *e = NULL, *entries_array = NULL;
   int present, allowed;
   enum slab_action action;
   uint64_t found_hash = 0, *found_hashes = NULL, batch_size = 1;
   uint64_t max_snapshot = get_snapshot_version(callback->transaction);
   uint64_t transaction_id = get_transaction_id(callback->transaction);

   /* Get the item location from the index */
   action = callback->action;
   present = 0; // Was the item in the index?
   allowed = -1; // Are we allowed to perform the action?
   switch(action) {
      case ADD:
      case READ:
      case UPDATE_IN_PLACE:
      case ADD_OR_UPDATE_IN_PLACE:
      case END_TRANSACTION_COMMIT:
      case START_TRANSACTION_COMMIT:
         // For these actions, we check if the item is in the index without taking a lock on the item
         // We do a lookup for the ADD because we want to make sure the item is not there before ADDing it (use ADD_OR_UPDATE_IN_PLACE or UPDATE to avoid the check)
         // The in place updates are not safe with respect to transactions (i.e., no isolation guarantees when the user uses them)
         e = memory_index_lookup(ctx->worker_id, callback, callback->item, max_snapshot, &allowed);
         break;
      case READ_NEXT:
         e = memory_index_lookup_next(ctx->worker_id, callback, callback->item, &found_hash, max_snapshot, &allowed);
         callback->next_key = found_hash;
         break;
      case READ_NEXT_BATCH:
         batch_size = (max_extra_io >= MAX_BATCH_SIZE)?MAX_BATCH_SIZE:(1+max_extra_io);
         //batch_size = MAX_BATCH_SIZE;
         entries_array = memory_index_read_next_batch(ctx->worker_id, callback, callback->item, max_snapshot, &batch_size, &found_hashes, &allowed);
         register_next_batch(callback, batch_size, found_hashes);
         break;
      case UPDATE:
      case READ_NO_LOOKUP:
         // No need to do a lookup for an UPDATE because it starts by ADDing
         break;
      case READ_FOR_WRITE:
      case LOCK:
         // called by a write in a transaction -- doesn't actually write, but locks the index
         memory_index_lookup_and_lock(ctx->worker_id, callback->item, callback, transaction_id, &present, &allowed);
         if(!present) // Item is not in DB, but we must create it to avoid future conflicts!
            memory_index_reserve(ctx->worker_id, callback->item, transaction_id);
         break;
      case REVERT:
         // Revert an item to the last valid version from snapshots. Called on transaction abort.
         memory_index_revert(ctx->worker_id, callback->item, callback, transaction_id);
         call_callback(callback, NULL);
         goto end;
      default:
         die("Action %d -- not sure what to do with the index\n", action);
         //e = memory_index_lookup_and_lock(ctx->worker_id, callback->item, callback, transaction_id, &present, &action_allowed);
         break;
   }


   /* Check if item is locked by a transaction or is too recent to to be read / written */
   if(allowed == -1) // we haven't checked permissions yet
      allowed = action_allowed(e, callback);
   if(!allowed) {
      callback->failed = 1;
      callback->slab = NULL;
      callback->slab_idx = -1;
      call_callback(callback, NULL);
      goto end;
   }

   /* Now perform the actual data related action */
   switch(action) {
      case LOCK:
         call_callback(callback, NULL);
         break;

      case READ_NO_LOOKUP:
         read_item_async(callback);
         break;

      /* Reads */
      case READ:
      case READ_NEXT:
      case READ_FOR_WRITE:
         if(!e) { // Item is not in DB
            callback->slab = NULL;
            callback->slab_idx = -1;
            call_callback(callback, NULL);
         } else {
            callback->slab = e->slab;
            callback->slab_idx = e->slab_idx;
            read_item_async(callback);
         }
         break;

      case READ_NEXT_BATCH:
         if(batch_size) {
            if(batch_size > 1) {
               for(size_t i = 0; i < batch_size - 1; i++) {
                  struct slab_callback *ncb = clone_callback(callback);
                  ncb->item = NULL; // no need to clone the item, but set to NULL to avoid accidentally freeing it later!
                  ncb->action = READ_NEXT_BATCH_CLONE;
                  ncb->slab = entries_array[i].slab;
                  ncb->slab_idx = entries_array[i].slab_idx;
                  ncb->next_key = found_hashes[i];
                  assert(ncb->next_key);
                  read_item_async(ncb);
               }
            }
            callback->slab = entries_array[batch_size-1].slab;
            callback->slab_idx = entries_array[batch_size-1].slab_idx;
            callback->next_key = found_hashes[batch_size-1];
            read_item_async(callback);
         } else {
            callback->slab = NULL;
            callback->slab_idx = -1;
            call_callback(callback, NULL);
         }
         free(entries_array);
         //free(found_hashes); // will be freed by the callback once the batch has been seen
         break;

      /* Adds */
      case ADD:
      case START_TRANSACTION_COMMIT:
         if(e) {
            print_item(e->slab_idx, callback->item);
            if(action == START_TRANSACTION_COMMIT)
               die("Adding a transaction that is already in the database! (This error might also appear if 2 keys have the same prefix, TODO: make index more robust to that.)\n");
            else
               die("Adding item that is already in the database! Use update instead! (This error might also appear if 2 keys have the same prefix, TODO: make index more robust to that.)\n");
         } else {
            //callback->action = ADD;
            if(action == ADD)
               callback->slab = get_slab(ctx, callback->item);
            else
               callback->slab = ctx->transactions_slab; // Transaction log is written in a special slab that is recovered first
            callback->slab_idx = -1;
            add_item_async(callback);
         }
         break;

      /* In place modifications */
      case UPDATE_IN_PLACE:
         if(!e) {
            callback->slab = NULL;
            callback->slab_idx = -1;
            call_callback(callback, NULL);
         } else {
            callback->slab = e->slab;
            callback->slab_idx = e->slab_idx;
            assert(get_item_size(callback->item) <= e->slab->item_size); // Item grew, we cannot update in place!
            update_in_place_item_async(callback);
         }
         break;
      case ADD_OR_UPDATE_IN_PLACE:
         if(!e) {
            callback->slab = get_slab(ctx, callback->item);
            callback->slab_idx = -1;
            callback->action = ADD;
            add_item_async(callback);
         } else {
            callback->slab = e->slab;
            callback->slab_idx = e->slab_idx;
            callback->action = UPDATE_IN_PLACE;
            update_in_place_item_async(callback);
         }
         break;

      /* Not in place add or update */
      case UPDATE:
         callback->slab = get_slab(ctx, callback->item);
         callback->slab_idx = -1;
         callback->needs_cleanup = 1;
         update_item_async(callback);
         break;

      /* Deletes */
      case DELETE:
         die("Not implemented");
         /* To implement:
          * - Do a special update that would write a tombstone to disk for the key... But this is a bit weird and probably doesn't play very well with our current assumption that deleted = (key_size == -1)
          * - Also, make sure it plays nicely with snapshots somehow...
          */
         break;

      case END_TRANSACTION_COMMIT:
         if(!e) {
            die("Cannot delete item or end transaction commit because item is not in the index?!\n");
         } else {
            callback->slab = e->slab;
            callback->slab_idx = e->slab_idx;
            add_item_in_partially_freed_list(e->slab, e->slab_idx, get_rdt_value(e));
            memory_index_delete(ctx->worker_id, callback->item);
            call_callback(callback, NULL);
            //remove_item_async(callback);
         }
         break;
      default:
         die("Unknown action\n");
   }
end:
   return batch_size - 1;
}

static void worker_do_cleaning(struct slab_context *ctx) {
   do_deletions(ctx->worker_id, ctx->gc);
}

/* Dequeue requests from a slab context */
static void worker_dequeue_requests(struct slab_context *ctx) {
   struct cb_queue *q = &ctx->cb_queue;
   struct slab_callback *head = NULL;
   if(q->nb_callbacks == 0)
      return;

   pthread_mutex_lock(&q->lock);
   int need_to_wakeup_injectors = (q->nb_callbacks == q->max_pending_callbacks); // queue was full, injectors might be waiting

   uint64_t to_dequeue = q->nb_callbacks;
   if(NEVER_EXCEED_QUEUE_DEPTH && (io_pending(ctx->io_ctx) + to_dequeue > QUEUE_DEPTH)) {
      to_dequeue = QUEUE_DEPTH - io_pending(ctx->io_ctx);
      if(to_dequeue) {
         head = q->head;
         struct slab_callback *tmp = head, *prev = NULL;
         for(size_t i = 0; i < to_dequeue; i++) {
            prev = tmp;
            tmp = tmp->next;
         }
         assert(tmp);
         prev->next = NULL;
         q->head = tmp;
         q->nb_callbacks -= to_dequeue;
         q->nb_total_processed_callbacks += to_dequeue;
      }
   } else {
      head = q->head;
      q->head = q->tail = NULL;
      q->nb_total_processed_callbacks += q->nb_callbacks;
      q->nb_callbacks = 0;
   }

   if(need_to_wakeup_injectors)
      pthread_cond_broadcast(&q->cond); // wakeup all injectors blocked in wait_for_free_spot_keep_lock()
   pthread_mutex_unlock(&q->lock);

   int max_extra_io;
   if(NEVER_EXCEED_QUEUE_DEPTH) {
      max_extra_io = QUEUE_DEPTH - to_dequeue;
   } else {
      max_extra_io = ctx->cb_queue.max_pending_callbacks - to_dequeue;
   }
   while(head) {
      struct slab_callback *next = head->next;
      head->next = NULL;

      int extra_ios_done = worker_do_one_request(ctx, head, max_extra_io);
      max_extra_io -= extra_ios_done;
      head = next;
   }
}


/*
 * Recovery procedures
 */

/* First we read the transaction commit file to check for partially committed transactions */
static void worker_slab_init_trans_cb(struct slab_callback *cb, void *item) {
   struct item_metadata *new_meta = item;

   printf("#WARNING: A transaction was not fully committed, has the database crashed? ID of transaction: %lu\n", new_meta->rdt);
   pthread_mutex_lock(&transaction_recovery_context_lock);
   transaction_recovery_context.nb_ignored_rdts++;
   transaction_recovery_context.ignored_rdts = realloc(transaction_recovery_context.ignored_rdts, transaction_recovery_context.nb_ignored_rdts*sizeof(*transaction_recovery_context.ignored_rdts));
   transaction_recovery_context.ignored_rdts[transaction_recovery_context.nb_ignored_rdts-1] = new_meta->rdt;
   pthread_mutex_unlock(&transaction_recovery_context_lock);
}

/* Function used to ignore partially committed transactions */
static int item_is_part_of_ignored_transaction(struct slab_callback *cb, void *item) {
   struct item_metadata *new_meta = item;
   if(transaction_recovery_context.nb_ignored_rdts == 0)
      return 0;
   for(size_t i = 0; i < transaction_recovery_context.nb_ignored_rdts; i++) {
      if(new_meta->rdt == transaction_recovery_context.ignored_rdts[i]) {
         printf("#WARNING: ignoring an item partially committed by transaction %lu\n", new_meta->rdt);
         // TODO: the spot should be added in the free list!
         return 1;
      }
   }
   return 0;
}

/* Function called on all items stored in slabs during recovery */
static void worker_slab_init_cb(struct slab_callback *cb, void *item) {
   struct item_metadata *new_meta = item;
   if(item_is_part_of_ignored_transaction(cb, item)) {
      // do nothing
   } else if(!memory_index_lookup(get_worker(cb->slab), NULL, item, -1, NULL)) { // item is non existant in the index => add it
      memory_index_add(cb, item);
   } else {
      /* Complex path -- item is already in the index, we should decide which one to keep based on rdt! */
      //printf("#WARNING! Item is present twice in the database! Has the database crashed?\n");
      struct item_metadata *old_meta = kv_read_sync(item);
      assert(old_meta);

      if(old_meta->rdt < new_meta->rdt) {
         // TODO: the old spot should be added in the freelist
         //kv_delete_sync(old_meta);
         memory_index_delete(get_worker(cb->slab), old_meta);
         memory_index_add(cb, item);
      } else {
         //kv_delete_sync(new_meta);
      }
   }
}

int is_worker_context(void) {
   return worker_context; // 1 if worker context, 0 if injector context
}

static void *worker_slab_init(void *pdata) {
   struct slab_context *ctx = pdata;

   worker_context = 1;
   worker_id = __sync_fetch_and_add(&nb_workers_launched, 1);

   pid_t x = syscall(__NR_gettid);
   printf("[SLAB WORKER %lu] tid %d\n", ctx->worker_id, x);
   pin_me_on(ctx->worker_id);

   /* Create the pagecache for the worker */
   ctx->pagecache = calloc(1, sizeof(*ctx->pagecache));
   page_cache_init(ctx->pagecache);

   /* Initialize the async io for the worker */
   ctx->io_ctx = worker_ioengine_init(ctx->worker_id, ctx->cb_queue.max_pending_callbacks);
   //ctx->cb_queue.max_pending_callbacks -= 40;

   /* Initialize the GC */
   ctx->gc = init_gc();

   /* Detect partially committed transactions */
   struct slab_callback *trans_cb = malloc(sizeof(*trans_cb));
   trans_cb->cb = worker_slab_init_trans_cb;
   ctx->transactions_slab = create_transactions_slab(ctx, ctx->worker_id, trans_cb);
   pthread_barrier_wait(&transaction_barrier);

   /* Rebuild the index by scanning all the slabs */
   size_t nb_slabs = sizeof(slab_sizes)/sizeof(*slab_sizes);
   ctx->slabs = malloc(nb_slabs*sizeof(*ctx->slabs));
   struct slab_callback *cb = new_slab_callback();
   cb->cb = worker_slab_init_cb;
   for(size_t i = 0; i < nb_slabs; i++) {
      ctx->slabs[i] = create_slab(ctx, ctx->worker_id, slab_sizes[i], cb);
   }
   free(cb);

   set_highest_rdt(ctx->rdt);
    __sync_add_and_fetch(&nb_workers_ready, 1);
   pthread_barrier_wait(&slab_barrier);

   /* Main loop: do IOs and process enqueued requests */
   declare_breakdown;
   while(1) {
      while(io_pending(ctx->io_ctx)) {
         worker_ioengine_enqueue_ios(ctx->io_ctx); __1
         //worker_dequeue_cleaning(ctx); // while we wait for IO, dequeue cleaning
         worker_ioengine_get_completed_ios(ctx->io_ctx); __2
         worker_ioengine_process_completed_ios(ctx->io_ctx); __3
      }

      volatile size_t pending = get_nb_pending_callbacks(&ctx->cb_queue);
      while(!pending && !io_pending(ctx->io_ctx)) {
         ctx->idle = 1;
         if(!PINNING || !SPINNING) {
            wait_for_requests(&ctx->cb_queue);
         } else {
            NOP10();
         }
         pending = get_nb_pending_callbacks(&ctx->cb_queue);
      } __4
      ctx->idle = 0;

      worker_do_cleaning(ctx); __5

      ctx->rdt = get_highest_rdt();
      worker_dequeue_requests(ctx); __6 // Process queue

      show_breakdown_periodic(1000, ctx->cb_queue.nb_total_processed_callbacks, "io_submit", "io_getevents", "io_cb", "wait", "gc", "slab_cb", " [GC - %lu elements]", gc_size(ctx->gc));
   }

   return NULL;
}

void slab_workers_init(int _nb_disks, int nb_workers_per_disk) {
   size_t max_pending_callbacks = MAX_NB_PENDING_CALLBACKS_PER_WORKER;
   nb_disks = _nb_disks;
   nb_workers = nb_disks * nb_workers_per_disk;

   pthread_barrier_init(&slab_barrier, NULL, nb_workers+1);
   pthread_barrier_init(&transaction_barrier, NULL, nb_workers);
   pthread_mutex_init(&transaction_recovery_context_lock, NULL);
   pthread_mutex_init(&biggest_rdt_lock, NULL);
   memory_index_init();

   pthread_t t;
   slab_contexts = calloc(nb_workers, sizeof(*slab_contexts));
   for(size_t w = 0; w < nb_workers; w++) {
      struct slab_context *ctx = &slab_contexts[w];
      ctx->worker_id = w;

      memset(&ctx->cb_queue, 0, sizeof(ctx->cb_queue));
      pthread_mutex_init(&ctx->cb_queue.lock, NULL);
      pthread_cond_init(&ctx->cb_queue.cond, NULL);
      ctx->cb_queue.max_pending_callbacks = max_pending_callbacks;

      pthread_create(&t, NULL, worker_slab_init, ctx);
   }

   pthread_barrier_wait(&slab_barrier);
}

size_t get_database_size(void) {
   uint64_t size = 0;
   size_t nb_slabs = sizeof(slab_sizes)/sizeof(*slab_sizes);

   size_t nb_workers = get_nb_workers();
   for(size_t w = 0; w < nb_workers; w++) {
      struct slab_context *ctx = &slab_contexts[w];
      for(size_t i = 0; i < nb_slabs; i++) {
         size += ctx->slabs[i]->nb_items;
      }
   }

   return size;
}

size_t pending_work(void) {
   uint64_t pending = 0;
   size_t nb_slabs = sizeof(slab_sizes)/sizeof(*slab_sizes);

   size_t nb_workers = get_nb_workers();
   for(size_t w = 0; w < nb_workers; w++) {
      struct slab_context *ctx = &slab_contexts[w];
      pending += !ctx->idle;
      for(size_t i = 0; i < nb_slabs; i++) {
         pending += io_pending(ctx->io_ctx);
         pending += get_nb_pending_callbacks(&ctx->cb_queue);
      }
   }

   return pending;
}
