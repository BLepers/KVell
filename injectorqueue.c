#include "headers.h"

/*
 * Injector queues are a way to switch between worker context and injector context.
 * By default the callbacks are called in worker context. It avoids going back & forth between workers and injectors.
 * However, workers CANNOT enqueue callbacks. So it is sometimes desirable to switch back to injector context.
 *
 *   q = create_new_injector_queue();
 *   cb = new_slab_callback();
 *   cb->injector_queue = q;
 *   cb->cb = do_something;
 *   kv_...(cb);
 *   ...
 *   injector_process_queue(q); // will call do_something, useful if do_something does reads or writes
 *
 *
 *   SAFE_INJECTOR_QUEUES option:
 *      When callbaks are called in worker context, they get a reference to the page cache and not a copy of the tuple they want to read.
 *      I.e.:   callback(cb, item) <- item points to the page cache
 *      It is safe because the page cache will not change while the callback is called in the worker context.
 *      With injector context, the content of the page cache might change between the enqueue and the processing of the callback.
 *      SAFE_INJECTOR_QUEUES clones the content of the page cache and give a copy to the user.
 */


struct per_core_injector_queue {
   pthread_mutex_t lock;
   struct slab_callback *head;
   struct slab_callback *tail;
};
struct injector_queue {
   struct per_core_injector_queue *queues;
};

void injector_insert_in_queue(int worker_id, struct injector_queue *_q, struct slab_callback *cb, void *item) {
   //cb->returned_item = clone_item(item); // we have to clone the item because it might disappear from the page cache between now and
   cb->returned_item = item;

   struct per_core_injector_queue *q = &_q->queues[worker_id];
   pthread_mutex_lock(&q->lock);
   assert(_q == cb->injector_queue);
   if(cb->action == READ_NEXT_BATCH_CLONE)
      assert(cb->slab);
   cb->next = NULL;
   if(!q->head) {
      q->head = cb;
      q->tail = cb;
   } else {
      q->tail->next = cb;
      q->tail = cb;
   }
   pthread_mutex_unlock(&q->lock);
}

int injector_process_queue(struct injector_queue *_q) {
   int processed = 0;
   struct slab_callback *cb = NULL;
   size_t nb_workers = get_nb_workers();

   for(size_t i = 0; i < nb_workers; i++) {
      struct per_core_injector_queue *q = &_q->queues[i];
      if(!(volatile void *)q->head)
         continue;

      pthread_mutex_lock(&q->lock);
      cb = q->head;
      q->head = NULL;
      q->tail = NULL;
      pthread_mutex_unlock(&q->lock);

      while(cb) {
         struct slab_callback *next = cb->next;
         //void *item = cb->returned_item;
         if(cb->cb)
            check_races_and_call(cb, cb->returned_item);
         //if(item && (SAFE_INJECTOR_QUEUES || TRANSACTION_TYPE == TRANS_LONG))
         //free(item);
         cb = next; // cb might have been freed by the call to the callback!
         processed++;
      }
   }
   return processed;
}

int injector_has_pending_work(struct injector_queue *_q) {
   size_t nb_workers = get_nb_workers();
   for(size_t i = 0; i < nb_workers; i++) {
      struct per_core_injector_queue *q = &_q->queues[i];
      if(q->head)
         return 1;
   }
   return 0;
}

void injector_print_queue_of(struct injector_queue *_q, uint64_t transaction_id) {
   size_t nb_workers = get_nb_workers();
   for(size_t i = 0; i < nb_workers; i++) {
      struct per_core_injector_queue *q = &_q->queues[i];
      struct slab_callback *cb = NULL;
      pthread_mutex_lock(&q->lock);
      cb = q->head;
      while(cb) {
         if(get_transaction_id(cb->transaction) == transaction_id) {
            printf("\tQueue trans %lu - Key %lu RDT %lu\n", transaction_id, item_get_key(cb->returned_item), item_get_rdt(cb->returned_item));
         }
         cb = cb->next;
      }
      pthread_mutex_unlock(&q->lock);
   }
}

struct injector_queue *create_new_injector_queue(void) {
   struct injector_queue *q = calloc(1, sizeof(*q));
   q->queues = calloc(get_nb_workers(), sizeof(*q->queues));
   for(size_t i = 0; i < get_nb_workers(); i++)
      pthread_mutex_init(&q->queues[i].lock, NULL);
   return q;
}
