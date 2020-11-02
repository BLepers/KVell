#ifndef SLAB_H
#define SLAB_H 1

#include "ioengine.h"

struct slab;
struct slab_callback;


struct slab {
   struct slab_context *ctx;

   size_t item_size;
   size_t nb_items;   // Number of non freed items
   size_t last_item;  // Total number of items, including freed
   size_t nb_max_items;

   int fd;
   size_t size_on_disk;

   size_t nb_partially_freed_items;
   struct freelist_entry *partially_freed_items;
};


/*
 * This is the callback enqueued in the engine.
 * Usage:
 *    callback = new_slab_callback();
 *    callback->item = ...; // item to look for or to write
 *    callback->cb = function to call once the read or write is completed
 *    callback->payload = anything you may need as a continuation
 *    callback->injector_queue = see injectorqueue.c
 *    kv_read_async(callback);
 */
typedef void (slab_cb_t)(struct slab_callback *, void *item);
enum slab_action { ADD, UPDATE, DELETE, READ, READ_FOR_WRITE, READ_NO_LOOKUP, ADD_OR_UPDATE_IN_PLACE, UPDATE_IN_PLACE, START_TRANSACTION_COMMIT, END_TRANSACTION_COMMIT, LOCK, REVERT, READ_NEXT, READ_NEXT_BATCH, READ_NEXT_BATCH_CLONE, MAP };
struct slab_callback {
   slab_cb_t *cb;                            // Function called once the item is read/written
   void *payload;                            // Payload for the callback, can contain anything
   void *payload2;                           // Payload for the callback, can contain anything
   void *item;                               // Item searched for.

   int raced;                                // Did we race? (Concurrent update modified the value we wanted to read?)
   int failed;                               // Only with transactions -- set to 1 if an item cannot be read / written
   struct injector_queue *injector_queue;    // Injector queue context -- instead of executing the callback, the worker thread will enqueue it in that context. Useful if the callback needs to be executed by a particular thread.
                                             // This is MANDATORY if the callback enqueues new callbacks.


   // Private

   // Location of item on disk
   enum slab_action action;                  // ADD, UPDATE, ...
   struct slab *slab;                        // In which file is the item?
   uint64_t slab_idx;                        // And at which index?
   struct lru *lru_entry;                    // Reference in the pagecache

   // Transaction
   struct transaction *transaction;          // Transaction context
   uint64_t snapshot_id;                     // Used for cleaning -- remove items up to this ID

   // Garbage collection
   int needs_cleanup;                        // Item had an old version
   int propagate_value_until;                // About to overwritte a value on disk ==> propagate to all transactions that can read the item & have a snapshot id < this field
   struct slab *old_slab;                    // When the update is NOT in place, where was the item before?
   uint64_t old_slab_idx;                    // And at which index?


   io_cb_t *io_cb;                           // This will be called by the IO engine once a page has been fetched from disk

   void *returned_item;                      // When READ'ing, this contains the item read
   struct slab_callback *next;               // Callbacks are enqueues in various queues (injector_queue or slabworker_queue)
   uint64_t next_key;                        // When reading the "next" item, this is the key that we are actually reading, used to detect races
   uint64_t max_next_key;                    // End of the scan
};
struct slab_callback *new_slab_callback(void);



struct slab* create_slab(struct slab_context *ctx, int worker_id, size_t item_size, struct slab_callback *callback);
struct slab* create_transactions_slab(struct slab_context *ctx, int worker_id, struct slab_callback *callback);
struct slab* resize_slab(struct slab *s);

void *read_item(struct slab *s, size_t idx); // unsafe
void write_item(struct slab *s, size_t idx, void *data); // unsafe

void read_item_async(struct slab_callback *callback);
void add_item_async(struct slab_callback *callback);
void update_item_async(struct slab_callback *callback);
void update_in_place_item_async(struct slab_callback *callback);
void remove_item_async(struct slab_callback *callback);

off_t item_page_num(struct slab *s, size_t idx);
struct slab_callback *clone_callback(struct slab_callback *cb);
int callback_is_reading(struct slab_callback *callback);
#endif
