#ifndef WORKLOAD_COMMON_H
#define WORKLOAD_COMMON_H 1

#include "headers.h"
#include "items.h"
#include "slab.h"
#include "stats.h"
#include "utils.h"
#include "random.h"
#include "slabworker.h"
#include "in-memory-index-generic.h"

struct workload;
typedef enum available_bench {
   ycsb_a_uniform,
   ycsb_b_uniform,
   ycsb_c_uniform,
   ycsb_e_uniform,
   ycsb_a_zipfian,
   ycsb_b_zipfian,
   ycsb_c_zipfian,
   ycsb_e_zipfian,
   prod1,
   prod2,
} bench_t;

struct workload_api {
   int (*handles)(bench_t w); // do we handle that workload?
   void (*launch)(struct workload *w, bench_t b); // launch workload
   const char* (*name)(bench_t w); // pretty print the benchmark (e.g., "YCSB A - Uniform")
   const char* (*api_name)(void); // pretty print API name (YCSB or PRODUCTION)
   char* (*create_unique_item)(uint64_t uid, uint64_t max_uid); // allocate an item in memory and return it
};
extern struct workload_api YCSB;
extern struct workload_api PRODUCTION;

struct workload {
   struct workload_api *api;
   int nb_load_injectors;
   uint64_t nb_requests;
   uint64_t nb_items_in_db;

   const char *db_path;

   // Filled automatically
   int nb_workers;
   uint64_t nb_requests_per_thread;
};

void repopulate_db(struct workload *w);
void run_workload(struct workload *w, bench_t bench);

char *create_unique_item(size_t item_size, uint64_t uid);
void print_item(size_t idx, void* _item);

void show_item(struct slab_callback *cb, void *item);
void free_callback(struct slab_callback *cb, void *item);
void compute_stats(struct slab_callback *cb, void *item);
struct slab_callback *bench_cb(void);

struct workload_api *get_api(bench_t b);
#endif
