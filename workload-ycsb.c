/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"

static char *_create_unique_item_ycsb(uint64_t uid) {
   size_t item_size = 1024;
   return create_unique_item(item_size, uid);
}

static char *create_unique_item_ycsb(uint64_t uid, uint64_t max_uid) {
   return _create_unique_item_ycsb(uid);
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

/* YCSB A (or D), B, C */
static void _launch_ycsb(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   for(size_t i = 0; i < nb_requests; i++) {
      struct slab_callback *cb = bench_cb();
      if(zipfian)
         cb->item = _create_unique_item_ycsb(zipf_next());
      else
         cb->item = _create_unique_item_ycsb(uniform_next());
      if(random_get_put(test)) { // In these tests we update with a given probability
         kv_update_async(cb);
      } else { // or we read
         kv_read_async(cb);
      }
      periodic_count(1000, "YCSB Load Injector (%lu%%)", i*100LU/nb_requests);
   }
}

/* YCSB E */
static void _launch_ycsb_e(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   random_gen_t rand_next = zipfian?zipf_next:uniform_next;
   for(size_t i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { // In this test we update with a given probability
         struct slab_callback *cb = bench_cb();
         cb->item = _create_unique_item_ycsb(rand_next());
         kv_update_async(cb);
      } else {  // or we scan
         die("Old scan interface no longer works in transactional KVell");
      }
      periodic_count(1000, "YCSB Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
   }
}

/* Generic interface */
static void launch_ycsb(struct workload *w, bench_t b) {
   switch(b) {
      case ycsb_a_uniform:
         return _launch_ycsb(0, w->nb_requests_per_thread, 0);
      case ycsb_b_uniform:
         return _launch_ycsb(1, w->nb_requests_per_thread, 0);
      case ycsb_c_uniform:
         return _launch_ycsb(2, w->nb_requests_per_thread, 0);
      case ycsb_e_uniform:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 0);
      case ycsb_a_zipfian:
         return _launch_ycsb(0, w->nb_requests_per_thread, 1);
      case ycsb_b_zipfian:
         return _launch_ycsb(1, w->nb_requests_per_thread, 1);
      case ycsb_c_zipfian:
         return _launch_ycsb(2, w->nb_requests_per_thread, 1);
      case ycsb_e_zipfian:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 1);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
         return "YCSB A - Uniform";
      case ycsb_b_uniform:
         return "YCSB B - Uniform";
      case ycsb_c_uniform:
         return "YCSB C - Uniform";
      case ycsb_e_uniform:
         return "YCSB E - Uniform";
      case ycsb_a_zipfian:
         return "YCSB A - Zipf";
      case ycsb_b_zipfian:
         return "YCSB B - Zipf";
      case ycsb_c_zipfian:
         return "YCSB C - Zipf";
      case ycsb_e_zipfian:
         return "YCSB E - Zipf";
      default:
         return "??";
   }
}

static int handles_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
      case ycsb_b_uniform:
      case ycsb_c_uniform:
      case ycsb_e_uniform:
      case ycsb_a_zipfian:
      case ycsb_b_zipfian:
      case ycsb_c_zipfian:
      case ycsb_e_zipfian:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_ycsb(void) {
   return "YCSB";
}

struct workload_api YCSB = {
   .handles = handles_ycsb,
   .launch = launch_ycsb,
   .api_name = api_name_ycsb,
   .name = name_ycsb,
   .create_unique_item = create_unique_item_ycsb,
};
