#include "headers.h"
#include "workload-common.h"

// Create a new item for the database
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

static void launch_prod(struct workload *w, bench_t b) {
   declare_periodic_count;
   random_gen_t rand_next = (b==prod1)?(production_random1):(production_random2);

   uint64_t nb_requests = w->nb_requests_per_thread;
   for(size_t i = 0; i < nb_requests; i++) {
      struct slab_callback *cb = bench_cb();
      cb->item = create_unique_item_prod(rand_next(), w->nb_items_in_db);

      // 58% write 40% read 2% scan
      long random = uniform_next() % 100;
      if(random < 58) {
         kv_update_async(cb);
      } else if(random < 98) {
         kv_read_async(cb);
      } else {
         tree_scan_res_t scan_res = kv_init_scan(cb->item, uniform_next()%99+1);
         free(cb->item);
         free(cb);
         for(size_t j = 0; j < scan_res.nb_entries; j++) {
            cb = bench_cb();
            cb->item = create_unique_item_prod(scan_res.hashes[j], w->nb_items_in_db);
            kv_read_async_no_lookup(cb, scan_res.entries[j].slab, scan_res.entries[j].slab_idx);
         }
         free(scan_res.hashes);
         free(scan_res.entries);
      }
      periodic_count(1000, "Production Load Injector");
   }
}

/* Pretty printing */
static const char *name_prod(bench_t w) {
   switch(w) {
      case prod1:
         return "Production 1";
      case ycsb_b_uniform:
         return "Production 2";
      default:
         return "??";
   }
}

static int handles_prod(bench_t w) {
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
   .name = name_prod,
   .api_name = api_name_prod,
   .create_unique_item = create_unique_item_prod,
};
