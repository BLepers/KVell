/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"

#define NB_REQ_PER_TRANSACTION 10

static char *create_unique_item_prod(uint64_t uid, uint64_t max_uid) {
   size_t item_size;
   if(uid*100LU/max_uid < 1) // 1%
      item_size = 100;
   else if(uid*100LU/max_uid < 82) // 81% + 1%
      item_size = 400;
   else if(uid*100LU/max_uid < 98)
      item_size = 1024;
   else
      item_size = 4096;

   return create_unique_item(item_size, uid);
}

struct prodaction_payload {
   uint64_t completed_requests;
   uint64_t to_do;
};

static volatile uint64_t failed_prod, nb_commits_done, nb_commits_launched;
static void commit_cb(struct slab_callback *cb, void *item) {
   uint64_t end;
   rdtscll(end);
   add_timing_stat(end - get_start_time(cb->transaction));

   __sync_fetch_and_add(&nb_commits_done, 1);
   if(has_failed(cb->transaction))
      __sync_fetch_and_add(&failed_prod, 1);
   free(get_payload(cb->transaction));
   free(cb->transaction);
   free(cb);
}


static __thread size_t nb_queries;
static void compute_stats_prod(struct slab_callback *cb, void *item) {
   struct prodaction_payload *p = get_payload(cb->transaction);
   size_t done = __sync_add_and_fetch(&p->completed_requests, 1);
   nb_queries++;
   if(done == p->to_do) {
      struct slab_callback *new_cb = new_slab_callback();
      new_cb->cb = commit_cb;
      new_cb->transaction = cb->transaction;
      new_cb->injector_queue = cb->injector_queue;
      kv_commit(cb->transaction, new_cb);
   }

   free(cb->item);
   free(cb);
}


struct slab_callback *prod_bench_cb(void) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = compute_stats_prod;
   return cb;
}

struct prod_payload {
   size_t nb_scans;
   volatile int done;
};

static void prod_map(struct slab_callback *cb, void *item) {
   struct prod_payload *p = cb->payload;
   __sync_fetch_and_add(&p->nb_scans, 1);
   if(!item)
      p->done = 1;
}


/* YCSB A (or D), B, C */
static volatile int glob_nb_scans_done;
static struct slab_callback *start_background_prod(size_t uid, struct workload *w, struct injector_queue *q) {
   struct prod_payload *p = calloc(1, sizeof(*p));
   struct slab_callback *prod_cb = new_slab_callback();
   prod_cb->cb = prod_map;
   prod_cb->payload = p;
   prod_cb->injector_queue = q;
   prod_cb->item = create_key(0);
   prod_cb->max_next_key = w->nb_items_in_db;
   //size_t beginning = w->nb_items_in_db/4 * uid;
   //size_t end = w->nb_items_in_db/4 * (uid + 1);
   //size_t beginning = rand_between(0, w->nb_items_in_db - 1000000);
   //size_t end = beginning + 1000000LU;
   //prod_cb->item = create_key(beginning);
   //prod_cb->max_next_key = end;
   kv_long_scan(prod_cb); // Start a background prod!
   return prod_cb;
}

static int is_prod_complete(struct slab_callback *prod_cb) {
   if(!prod_cb)
      return 0;

   struct prod_payload *p = prod_cb->payload;
   if(p->done == 1) {
      p->done = 2;
      return 1;
   }
   return 0;
}

static size_t prod_progress(struct slab_callback *prod_cb) {
   if(!prod_cb)
      return 0;

   struct prod_payload *p = prod_cb->payload;
   return p->nb_scans;
}

struct injector_bg_work {
   size_t nb_scans;
   struct slab_callback **prod_cbs;
   int nb_scans_done;
   int uid;
   struct_declare_timer;
};

static size_t prod_progress_tot(struct injector_bg_work *p) {
   uint64_t tot = 0;
   for(size_t i = 0; i < p->nb_scans; i++) {
      tot += prod_progress(p->prod_cbs[i]);
   }
   return tot;
}

static void do_injector_bg_work(struct workload *w, struct injector_queue *q, struct injector_bg_work *p) {
   injector_process_queue(q);
   for(size_t i = 0; i < p->nb_scans; i++) {
      struct slab_callback *prod_cb = p->prod_cbs[i];
      if(is_prod_complete(prod_cb)) {
         struct_stop_timer(p, "prodning #%d %lu/%lu elements", p->nb_scans_done, prod_progress(prod_cb), get_database_size());
         p->nb_scans_done++;
         free(prod_cb->payload);
         free(prod_cb->item);
         free(prod_cb);
         p->prod_cbs[i] = NULL;
         __sync_add_and_fetch(&glob_nb_scans_done, 1);
      }
   }
}

static int injector_uid;
static void _launch_prod(int test, int nb_requests, int zipfian, struct workload *w) {
   struct injector_queue *q = create_new_injector_queue();
   int uid = __sync_fetch_and_add(&injector_uid, 1);

   struct injector_bg_work p = { .uid = uid };
   struct_start_timer(&p);

   if(uid == 0) {
      p.nb_scans = w->nb_scans;
      p.prod_cbs = calloc(w->nb_scans, sizeof(*p.prod_cbs));
      for(size_t i = 0; i < w->nb_scans; i++) {
         p.prod_cbs[i] = start_background_prod(uid, w, q);
      }
   } else {
      p.nb_scans = 0;
   }

   declare_periodic_count;
   size_t _nb_requests = 0;
   nb_queries = 0;
   while(glob_nb_scans_done != w->nb_scans || (w->nb_scans == 0 && _nb_requests < nb_requests)) {
      struct transaction *t = create_transaction();
      struct prodaction_payload *_p = calloc(1, sizeof(struct prodaction_payload));
      __sync_fetch_and_add(&nb_commits_launched, 1);
      set_payload(t, _p);

      // 58% write 40% read 2% scan
      long random = uniform_next() % 100;
      if(random < 98) {
         _p->to_do = NB_REQ_PER_TRANSACTION;
         for(size_t j = 0; j < NB_REQ_PER_TRANSACTION; j++) {
            struct slab_callback *cb = prod_bench_cb();
            cb->injector_queue = q;
            if(test == 0)
               cb->item = create_unique_item_prod(production_random1(), w->nb_items_in_db);
            else
               cb->item = create_unique_item_prod(production_random2(), w->nb_items_in_db);
            long is_update = uniform_next() % 100;
            if(is_update < 59) { // In these tests we update with a given probability
               kv_trans_write(t, cb);
            } else { // or we read
               kv_trans_read(t, cb);
            }
            _nb_requests++;
            do_injector_bg_work(w, q, &p);
         }
      } else {
         uint64_t nb_scans = uniform_next()%99+1;
         _p->to_do = nb_scans;
         for(size_t j = 0; j < nb_scans; j++) {
            struct slab_callback *cb = prod_bench_cb();
            cb->injector_queue = q;
            if(test == 0)
               cb->item = create_unique_item_prod(production_random1(), w->nb_items_in_db);
            else
               cb->item = create_unique_item_prod(production_random2(), w->nb_items_in_db);
            kv_trans_read(t, cb);
            _nb_requests++;
         }
      }
      periodic_count(1000, "PRODUCTION Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [%lu fail / %lu done / %lu launched] [%lu prods done - %d full / %d] [%lu queries done]", uid, failed_prod*100/nb_commits_done, get_snapshot_size(), failed_prod, nb_commits_done, nb_commits_launched, prod_progress_tot(&p), glob_nb_scans_done, w->nb_scans, nb_queries);
   }
   do {
      periodic_count(1000, "PRODUCTION Load Injector %d DONE [failure rate = %lu%%] [snapshot size %lu] [%lu fail / %lu done / %lu launched] [%lu prods done - %d full / %d] [%lu queries done]", uid, failed_prod*100/nb_commits_done, get_snapshot_size(), failed_prod, nb_commits_done, nb_commits_launched, prod_progress_tot(&p), glob_nb_scans_done, w->nb_scans, nb_queries);
      do_injector_bg_work(w, q, &p);
   } while(pending_work() || injector_has_pending_work(q));

   free(q);
}

/* Generic interface */
static void launch_prod(struct workload *w, bench_t b) {
   switch(b) {
      case prod1:
         return _launch_prod(0, w->nb_requests_per_thread, 0, w);
      case prod2:
         return _launch_prod(1, w->nb_requests_per_thread, 0, w);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_prod(bench_t w) {
   printf("PRODUCTION BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu]\n", ((double)failed_prod)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size());
   switch(w) {
      case prod1:
         return "PROD 1";
      case prod2:
         return "PROD 2";
      default:
         return "??";
   }
}

static int handles_prod(bench_t w) {
   failed_prod = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;
   nb_commits_launched = 0;
   glob_nb_scans_done = 0;
   injector_uid = 0;

   switch(w) {
      case prod1:
      case prod2:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_prod(void) {
   return "PRODUCTION";
}

struct workload_api PRODUCTION = {
   .handles = handles_prod,
   .launch = launch_prod,
   .api_name = api_name_prod,
   .name = name_prod,
   .create_unique_item = create_unique_item_prod,
};

