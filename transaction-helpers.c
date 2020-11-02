#include "headers.h"
#include "indexes/pqueue.h"

#define MAXIMUM_CONCURRENT_TRANSACTIONS 100000

/*
 * This file contains all the logic related to the list of all running transactions:
 * - register/terminate a transaction
 * - min active commit
 * - for OLCP transactions: propagation of items to all long running transactions
 * - etc.
 */

struct running_transaction {
	uint64_t transaction_id;
	uint64_t snapshot_id;
};
struct running_transaction_list {
   struct running_transaction *transactions;
   size_t head, tail;
   size_t max_avail;
   size_t max_recorded_parallel_transactions; // stats
} running_transactions;

static struct transaction *long_transactions_head, *long_transactions_tail;

#define tail(r) ((r).tail % (r).max_avail)
#define last(r) (((r).tail - 1)  % (r).max_avail)
#define arr_idx(r, i) ((i)  % (r).max_avail)
#define head(r) ((r).head % (r).max_avail)
#define next(r, pos) ((pos + 1) % (r).max_avail)
#define prev(r, pos) ( (pos == 0)?((r).max_avail - 1):(pos - 1) )

static pthread_mutex_t running_lock;

/*
 * New transaction
 */
uint64_t register_new_transaction(struct transaction *t) {
   uint64_t rdt, snapshot;
   struct running_transaction *r;

   pthread_mutex_lock(&running_lock);

   /*
    * 1/ Get the snapshot timestamp of the transaction
    */
   rdt = get_highest_rdt();         // current global timestamp of the KV
   snapshot = get_min_in_commit();  // Ignore every commit that is not yet fully committed
   if(snapshot == -1)               // If no transaction is in the process of committing
      snapshot = rdt;               // ... then still refuse reading anything more recent than our timestamp*/

   r = &running_transactions.transactions[tail(running_transactions)];
   r->transaction_id = rdt;
   r->snapshot_id = snapshot;

   set_transaction_id(t, rdt);
   set_snapshot_version(t, snapshot);

   /*
    * 2/ Insert the transaction in the list of running transactions
    */
   running_transactions.tail++;
   if(tail(running_transactions) == head(running_transactions)) {
      die("Maximum number of parallel transactions exceeded\n");
   } else {
      uint64_t pending = get_nb_running_transactions();
      if(pending > running_transactions.max_recorded_parallel_transactions)
         running_transactions.max_recorded_parallel_transactions = pending;
   }

   /*
    * 3/ Record all long running OLCP transactions in a separate structure
    */
   if(get_map(t)) { // Long running if it has a map function
      if(long_transactions_head) {
         transaction_set_long_prev(t, long_transactions_tail);
         transaction_set_long_next(t, NULL);
         transaction_set_long_next(long_transactions_tail, t);
         long_transactions_tail = t;
      } else {
         transaction_set_long_prev(t, NULL);
         transaction_set_long_next(t, NULL);
         long_transactions_head = t;
         long_transactions_tail = t;
      }
   }

   pthread_mutex_unlock(&running_lock);


   return rdt;
}

uint64_t get_max_recorded_parallel_transactions(void) {
   return running_transactions.max_recorded_parallel_transactions;
}

size_t get_nb_running_transactions(void) {
   return running_transactions.tail - running_transactions.head;
}


/*
 * End of a transaction (called after commit)
 */
static void register_end_running_transaction(size_t id) {
   size_t pos, start;
   struct running_transaction *r;

   pthread_mutex_lock(&running_lock);

   // Find the transaction in the transaction array
   start = head(running_transactions);
   pos = start;
   do {
      r = &running_transactions.transactions[pos];
      if(r->transaction_id == id)
         break;
      pos = next(running_transactions, pos);
      assert(pos != start); // bug if we looped == didn't find the transaction
   } while(1);

   if(pos != start) { // Shift the array so that it only contains running transactions
      while(pos != start) {
         size_t _prev = prev(running_transactions, pos);
         running_transactions.transactions[pos] = running_transactions.transactions[_prev];
         pos = _prev;
      }
   }
   memset(&running_transactions.transactions[start], 0, sizeof(running_transactions.transactions[start])); // clean the spot
   running_transactions.head++;

   pthread_mutex_unlock(&running_lock);
}

/*
 * Transactions just started commit, remove it from the long transaction list to avoid receiving further propagated items
 */
void register_start_commit(struct transaction *t) {
   if(get_map(t)) {
      pthread_mutex_lock(&running_lock);
      if(t == long_transactions_head)
         long_transactions_head = transaction_get_long_next(t);
      if(t == long_transactions_tail)
         long_transactions_tail = transaction_get_long_prev(t);
      if(transaction_get_long_next(t)) {
         struct transaction *next = transaction_get_long_next(t);
         transaction_set_long_prev(next, transaction_get_long_prev(t));
      }
      if(transaction_get_long_prev(t)) {
         struct transaction *prev = transaction_get_long_prev(t);
         transaction_set_long_next(prev, transaction_get_long_next(t));
      }
      pthread_mutex_unlock(&running_lock);
   }
}

/*
 * Helper to propagate an item to all long running transactions
 */
void forall_long_running_transaction(size_t min_rdt, size_t max_rdt, void *data) {
   // TODO/WARNING: thread safety?! Race with register_end_running_transaction + deletion of the transaction data structure?
   struct transaction *t = long_transactions_head;
   size_t nb = 0;
   while(t) {
      if(!is_in_commit(t) && get_snapshot_version(t) >= min_rdt && get_snapshot_version(t) < max_rdt) {
         //printf("Propagating key %lu rdt %lu to trans %lu [%lu <  %lu < %lu ?] %p\n", item_get_key(data), item_get_rdt(data), get_transaction_id(t), min_rdt, get_snapshot_version(t), max_rdt, get_map(t));
         get_map(t)->cb(get_map(t), data);
      } else {
         //printf("NOT Propagating key %lu rdt %lu to trans %lu [%lu <  %lu < %lu ?]\n", item_get_key(data), item_get_rdt(data), get_transaction_id(t), min_rdt, get_snapshot_version(t), max_rdt);
      }
      t = transaction_get_long_next(t);

      // Debug
      nb++;
      if(nb > 130)
         die("Likely infinite loop issue, or did you have more than 130 long running transactions?");
   }
}

/*
 * Get the snapshot ID of the oldest transaction.
 * TODO: compute the min in register_new_transaction!
 */
uint64_t get_min_snapshot_id(void) {
   uint64_t snapshot_id;
   //pthread_mutex_lock(&running_lock);
   if(tail(running_transactions) == head(running_transactions)) {
      snapshot_id = get_highest_rdt();
   } else {
      snapshot_id = running_transactions.transactions[head(running_transactions)].snapshot_id;
   }
   //pthread_mutex_unlock(&running_lock);
   return snapshot_id;
}

/*
 * Helpers for transactions in a commit
 */
static pthread_mutex_t in_commit_lock;
static pqueue_t *in_commit;

typedef struct node_t {
   pqueue_pri_t transaction_id;
   size_t pos;
} node_t;


static int cmp_pri(pqueue_pri_t next, pqueue_pri_t curr) {
   return (next > curr);
}

static pqueue_pri_t get_pri(void *a) {
   return ((node_t *) a)->transaction_id;
}

static void set_pri(void *a, pqueue_pri_t pri) {
   ((node_t *) a)->transaction_id = pri;
}

static size_t get_pos(void *a) {
   return ((node_t *) a)->pos;
}

static void set_pos(void *a, size_t pos) {
   ((node_t *) a)->pos = pos;
}

size_t register_commit_transaction(size_t id) {
   uint64_t rdt;
   struct node_t *n = malloc(sizeof(*n));
   pthread_mutex_lock(&in_commit_lock);
   rdt = get_highest_rdt();
   n->transaction_id = rdt;
   pqueue_insert(in_commit, n);
   pthread_mutex_unlock(&in_commit_lock);
   return rdt;
}


size_t get_min_in_commit(void) {
   struct node_t *min;
   pthread_mutex_lock(&in_commit_lock);
   min = pqueue_peek(in_commit);
   pthread_mutex_unlock(&in_commit_lock);
   return min?min->transaction_id:-1;
}

void register_end_transaction(size_t id, size_t id_on_disk) {
   if(id_on_disk) { // only true if the transaction didn't fail, otherwise it never registered itself as "committing"
      pthread_mutex_lock(&in_commit_lock);
      pqueue_find_and_remove(in_commit, id_on_disk);
      pthread_mutex_unlock(&in_commit_lock);
   }

   register_end_running_transaction(id);
}

/*
 * Init function, must be called before creating transactions
 */
void init_transaction_manager(void) {
   pthread_mutex_init(&running_lock, NULL);
   running_transactions.transactions = calloc(MAXIMUM_CONCURRENT_TRANSACTIONS, sizeof(*running_transactions.transactions));
   running_transactions.max_avail = MAXIMUM_CONCURRENT_TRANSACTIONS;
   pthread_mutex_init(&in_commit_lock, NULL);
   in_commit = pqueue_init(200, cmp_pri, get_pri, set_pri, get_pos, set_pos);
}
