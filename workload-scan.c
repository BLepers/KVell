/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"

#define NB_REQ_PER_TRANSACTION 16

static char *_create_unique_item_scan(uint64_t uid) {
   size_t item_size = 1024;
   //size_t item_size = 4024;
   return create_unique_item(item_size, uid);
}

static char *create_unique_item_scan(uint64_t uid, uint64_t max_uid) {
   return _create_unique_item_scan(uid);
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
      case 3: // D
         return 1;
   }
   die("Not a valid test\n");
}

struct scanaction_payload {
   uint64_t completed_requests;
};

static volatile uint64_t failed_scan, nb_commits_done, nb_commits_launched;
static void commit_cb(struct slab_callback *cb, void *item) {
   uint64_t end;
   rdtscll(end);
   add_timing_stat(end - get_start_time(cb->transaction));

   __sync_fetch_and_add(&nb_commits_done, 1);
   if(has_failed(cb->transaction))
      __sync_fetch_and_add(&failed_scan, 1);
   free(get_payload(cb->transaction));
   free(cb->transaction);
   free(cb);
}


static __thread size_t nb_queries;
static void compute_stats_scan(struct slab_callback *cb, void *item) {
   struct scanaction_payload *p = get_payload(cb->transaction);
   size_t done = __sync_add_and_fetch(&p->completed_requests, 1);
   nb_queries++;
   if(done == NB_REQ_PER_TRANSACTION) {
      struct slab_callback *new_cb = new_slab_callback();
      new_cb->cb = commit_cb;
      new_cb->transaction = cb->transaction;
      new_cb->injector_queue = cb->injector_queue;
      kv_commit(cb->transaction, new_cb);
   }

   free(cb->item);
   free(cb);
}


struct slab_callback *scan_bench_cb(void) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = compute_stats_scan;
   return cb;
}

struct scan_payload {
   size_t nb_scans;
   volatile int done;
};

static void scan_map(struct slab_callback *cb, void *item) {
   struct scan_payload *p = cb->payload;
   __sync_fetch_and_add(&p->nb_scans, 1);
   if(!item)
      p->done = 1;
}


/* YCSB A (or D), B, C */
static volatile int glob_nb_scans_done;
static struct slab_callback *start_background_scan(size_t uid, struct workload *w, struct injector_queue *q) {
   struct scan_payload *p = calloc(1, sizeof(*p));
   struct slab_callback *scan_cb = new_slab_callback();
   scan_cb->cb = scan_map;
   scan_cb->payload = p;
   scan_cb->injector_queue = q;
   scan_cb->item = create_key(uid*w->nb_items_in_db/4);
   scan_cb->max_next_key = (uid+1)*w->nb_items_in_db/4;
   //size_t beginning = w->nb_items_in_db/4 * uid;
   //size_t end = w->nb_items_in_db/4 * (uid + 1);
   //size_t beginning = rand_between(0, w->nb_items_in_db - 1000000);
   //size_t end = beginning + 1000000LU;
   //scan_cb->item = create_key(beginning);
   //scan_cb->max_next_key = end;
   kv_long_scan(scan_cb); // Start a background scan!
   return scan_cb;
}

static int is_scan_complete(struct slab_callback *scan_cb) {
   if(!scan_cb)
      return 0;

   struct scan_payload *p = scan_cb->payload;
   if(p->done == 1) {
      p->done = 2;
      return 1;
   }
   return 0;
}

static size_t scan_progress(struct slab_callback *scan_cb) {
   if(!scan_cb)
      return 0;

   struct scan_payload *p = scan_cb->payload;
   return p->nb_scans;
}

struct injector_bg_work {
   size_t nb_scans;
   struct slab_callback **scan_cbs;
   int nb_scans_done;
   int uid;
   struct_declare_timer;
};

static size_t scan_progress_tot(struct injector_bg_work *p) {
   uint64_t tot = 0;
   for(size_t i = 0; i < p->nb_scans; i++) {
      tot += scan_progress(p->scan_cbs[i]);
   }
   return tot;
}

static void do_injector_bg_work(struct workload *w, struct injector_queue *q, struct injector_bg_work *p) {
   injector_process_queue(q);
   for(size_t i = 0; i < p->nb_scans; i++) {
      struct slab_callback *scan_cb = p->scan_cbs[i];
      if(is_scan_complete(scan_cb)) {
         struct_stop_timer(p, "Scanning #%d %lu/%lu elements", p->nb_scans_done, scan_progress(scan_cb), get_database_size());
         p->nb_scans_done++;
         free(scan_cb->payload);
         free(scan_cb->item);
         free(scan_cb);
         p->scan_cbs[i] = NULL;
         __sync_add_and_fetch(&glob_nb_scans_done, 1);
      }
   }
}

static int injector_uid;
static void _launch_scan(int test, int nb_requests, int zipfian, struct workload *w) {
   struct injector_queue *q = create_new_injector_queue();
   int uid = __sync_fetch_and_add(&injector_uid, 1);

   struct injector_bg_work p = { .uid = uid };
   struct_start_timer(&p);

   int all = 1;
   if(all || uid == 0) {
      p.nb_scans = w->nb_scans;
      p.scan_cbs = calloc(w->nb_scans, sizeof(*p.scan_cbs));
      for(size_t i = 0; i < w->nb_scans; i++) {
         p.scan_cbs[i] = start_background_scan(uid, w, q);
      }
   } else {
      p.nb_scans = 0;
   }

   declare_periodic_count;
   size_t _nb_requests = 0;
   nb_queries = 0;
   size_t target_scans = all?(w->nb_scans * w->nb_load_injectors):w->nb_scans;
   while(glob_nb_scans_done != target_scans || (w->nb_scans == 0 && _nb_requests < nb_requests)) {
      struct transaction *t = create_transaction();
      __sync_fetch_and_add(&nb_commits_launched, 1);
      set_payload(t, calloc(1, sizeof(struct scanaction_payload)));

      for(size_t j = 0; j < NB_REQ_PER_TRANSACTION; j++) {
         struct slab_callback *cb = scan_bench_cb();
         cb->injector_queue = q;
         if(zipfian)
            cb->item = _create_unique_item_scan(zipf_next());
         else
            cb->item = _create_unique_item_scan(uniform_next());
         if(random_get_put(test)) { // In these tests we update with a given probability
            kv_trans_write(t, cb);
         } else { // or we read
            kv_trans_read(t, cb);
         }
         _nb_requests++;
         do_injector_bg_work(w, q, &p);
      }
      periodic_count(1000, "SCAN Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [%lu fail / %lu done / %lu launched] [%lu scans done - %d full / %lu] [%lu queries done]", uid, failed_scan*100/nb_commits_done, get_snapshot_size(), failed_scan, nb_commits_done, nb_commits_launched, scan_progress_tot(&p), glob_nb_scans_done, target_scans, nb_queries);
   }
   do {
      periodic_count(1000, "SCAN Load Injector %d DONE [failure rate = %lu%%] [snapshot size %lu] [%lu fail / %lu done / %lu launched] [%lu scans done - %d full / %lu] [%lu queries done]", uid, failed_scan*100/nb_commits_done, get_snapshot_size(), failed_scan, nb_commits_done, nb_commits_launched, scan_progress_tot(&p), glob_nb_scans_done, target_scans, nb_queries);
      do_injector_bg_work(w, q, &p);
   } while(pending_work() || injector_has_pending_work(q));

   free(q);
}

/* Generic interface */
static void launch_scan(struct workload *w, bench_t b) {
   switch(b) {
      case scan_a_uniform:
         return _launch_scan(0, w->nb_requests_per_thread, 0, w);
      case scan_b_uniform:
         return _launch_scan(1, w->nb_requests_per_thread, 0, w);
      case scan_c_uniform:
         return _launch_scan(2, w->nb_requests_per_thread, 0, w);
      case scan_d_uniform:
         return _launch_scan(3, w->nb_requests_per_thread, 0, w);
      case scan_a_zipfian:
         return _launch_scan(0, w->nb_requests_per_thread, 1, w);
      case scan_b_zipfian:
         return _launch_scan(1, w->nb_requests_per_thread, 1, w);
      case scan_c_zipfian:
         return _launch_scan(2, w->nb_requests_per_thread, 1, w);
      case scan_d_zipfian:
         return _launch_scan(3, w->nb_requests_per_thread, 1, w);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_scan(bench_t w) {
   printf("SCAN BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu]\n", ((double)failed_scan)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size());
   switch(w) {
      case scan_a_uniform:
         return "scan A - Uniform";
      case scan_b_uniform:
         return "scan B - Uniform";
      case scan_c_uniform:
         return "scan C - Uniform";
      case scan_d_uniform:
         return "scan D (write only) - Uniform";
      case scan_a_zipfian:
         return "scan A - Zipf";
      case scan_b_zipfian:
         return "scan B - Zipf";
      case scan_c_zipfian:
         return "scan C - Zipf";
      case scan_d_zipfian:
         return "scan D (write only) - Zipf";
      default:
         return "??";
   }
}

static int handles_scan(bench_t w) {
   failed_scan = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;
   nb_commits_launched = 0;
   glob_nb_scans_done = 0;
   injector_uid = 0;

   switch(w) {
      case scan_a_uniform:
      case scan_b_uniform:
      case scan_c_uniform:
      case scan_d_uniform:
      case scan_a_zipfian:
      case scan_b_zipfian:
      case scan_c_zipfian:
      case scan_d_zipfian:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_scan(void) {
   return "SCAN";
}

struct workload_api SCAN = {
   .handles = handles_scan,
   .launch = launch_scan,
   .api_name = api_name_scan,
   .name = name_scan,
   .create_unique_item = create_unique_item_scan,
};

