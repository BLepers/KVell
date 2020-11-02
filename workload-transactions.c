/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"

#define NB_REQ_PER_TRANSACTION 100

static char *_create_unique_item_trans(uint64_t uid) {
   size_t item_size = 1024;
   return create_unique_item(item_size, uid);
}

static char *create_unique_item_trans(uint64_t uid, uint64_t max_uid) {
   return _create_unique_item_trans(uid);
}

/* Is the current request a get or a put? */
static int random_get_put(int test) {
   long random = uniform_next() % 100;
   switch(test) {
      case 0: // A
         return random >= 50;
      case 1: // B
         return random >= 95;
      case 2: // C
         return 0;
      case 3: // E
         return random >= 95;
   }
   die("Not a valid test\n");
}

struct transaction_payload {
   uint64_t completed_requests;
};

static volatile uint64_t failed_trans, nb_commits_done, nb_commits_launched;
static uint64_t __reads, __reads_from_snapshot;
static void commit_cb(struct slab_callback *cb, void *item) {
   uint64_t end;
   rdtscll(end);
   add_timing_stat(end - get_start_time(cb->transaction));

   __sync_fetch_and_add(&nb_commits_done, 1);
   if(has_failed(cb->transaction))
      __sync_fetch_and_add(&failed_trans, 1);
   free(get_payload(cb->transaction));
   free(cb);
}


static void compute_stats_trans(struct slab_callback *cb, void *item) {
   struct transaction_payload *p = get_payload(cb->transaction);
   size_t done = __sync_add_and_fetch(&p->completed_requests, 1);
   if(done == NB_REQ_PER_TRANSACTION) {
      struct slab_callback *new_cb = new_slab_callback();
      new_cb->cb = commit_cb;
      new_cb->transaction = cb->transaction;
      new_cb->injector_queue = cb->injector_queue;
      kv_commit(cb->transaction, new_cb);
   }

   if(cb->action == READ)
      __reads++;
   //else if(cb->action == READ_FROM_SNAPSHOT)
   //__reads_from_snapshot++;

   free(cb->item);
   free(cb);
}


struct slab_callback *trans_bench_cb(void) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = compute_stats_trans;
   return cb;
}

extern size_t collect_stats, _print_stats;

/* YCSB A (or D), B, C */
static void _launch_trans(int test, int nb_requests, int zipfian) {
   struct injector_queue *q = create_new_injector_queue();

   create_transaction(); // screw up the snapshots!

   //collect_stats = 1;

   declare_periodic_count;
   for(size_t i = 0; i < nb_requests; ) {
      struct transaction *t = create_transaction();
      __sync_fetch_and_add(&nb_commits_launched, 1);
      set_payload(t, calloc(1, sizeof(struct transaction_payload)));

      for(size_t j = 0; j < NB_REQ_PER_TRANSACTION; j++) {
         struct slab_callback *cb = trans_bench_cb();
         cb->injector_queue = q;
         if(zipfian)
            cb->item = _create_unique_item_trans(zipf_next());
         else
            cb->item = _create_unique_item_trans(uniform_next());
         if(random_get_put(test)) { // In these tests we update with a given probability
            kv_trans_write(t, cb);
         } else { // or we read
            //kv_trans_read(t, cb);
         }
         i++;
         injector_process_queue(q);
      }
      periodic_count(1000, "TRANS Load Injector (%lu%%) [failure rate = %lu%%] [snapshot size %lu] [%f%% reads from snapshot (%lu / %lu)] [%lu fail / %lu done / %lu launched]", i*100LU/nb_requests, failed_trans*100/nb_commits_done, get_snapshot_size(), (__reads)?(__reads_from_snapshot*100./(__reads+__reads_from_snapshot)):-1., __reads_from_snapshot, __reads + __reads_from_snapshot, failed_trans, nb_commits_done, nb_commits_launched);
   }
   collect_stats = 0;
   //_print_stats = 1;
   do {
      injector_process_queue(q);
   } while(pending_work());
}

/* Generic interface */
static void launch_trans(struct workload *w, bench_t b) {
   switch(b) {
      case trans_a_uniform:
         return _launch_trans(0, w->nb_requests_per_thread, 0);
      case trans_b_uniform:
         return _launch_trans(1, w->nb_requests_per_thread, 0);
      case trans_c_uniform:
         return _launch_trans(2, w->nb_requests_per_thread, 0);
      case trans_a_zipfian:
         return _launch_trans(0, w->nb_requests_per_thread, 1);
      case trans_b_zipfian:
         return _launch_trans(1, w->nb_requests_per_thread, 1);
      case trans_c_zipfian:
         return _launch_trans(2, w->nb_requests_per_thread, 1);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_trans(bench_t w) {
   printf("TRANSACTION BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu]\n", ((double)failed_trans)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size());
   switch(w) {
      case trans_a_uniform:
         return "TRANS A - Uniform";
      case trans_b_uniform:
         return "TRANS B - Uniform";
      case trans_c_uniform:
         return "TRANS C - Uniform";
      case trans_a_zipfian:
         return "TRANS A - Zipf";
      case trans_b_zipfian:
         return "TRANS B - Zipf";
      case trans_c_zipfian:
         return "TRANS C - Zipf";
      default:
         return "??";
   }
}

static int handles_trans(bench_t w) {
   failed_trans = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;

   switch(w) {
      case trans_a_uniform:
      case trans_b_uniform:
      case trans_c_uniform:
      case trans_a_zipfian:
      case trans_b_zipfian:
      case trans_c_zipfian:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_trans(void) {
   return "TRANSACTIONS";
}

struct workload_api TRANS = {
   .handles = handles_trans,
   .launch = launch_trans,
   .api_name = api_name_trans,
   .name = name_trans,
   .create_unique_item = create_unique_item_trans,
};

