/*
 * TPCH workload
 */

#include "headers.h"
#include "workload-common.h"
#include "workload-tpch.h"

/*
 * File contains:
 * 1/ Definitions of tables and columns
 * 2/ Functions to generate keys for items stored in the KV (e.g., a unique key for an item or a warehouse)
 * 3/ Loader functions
 * 4/ Queries
 */

/*
 * TPCH has a few tables and columns:
 */
enum table { LINEITEM, PART, PARTSUPP, ORDERS, SUPPLIERS, CUSTOMER, NATION, REGION };

enum column {
   ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX, RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE, COMMENT,
};


/*
 * We want every DB insert to have a unique and identifiable key. These functions generate a unique key depending on the table, column and primary key(s) of a tuple.
 */
struct tpch_key {
   union {
      long key;
      struct {
         unsigned long prim_key:60;
         unsigned int table:4;
      };
   };
} __attribute__((packed));

long get_key_lineitem(long item) {
   struct tpch_key k = {
      .table = LINEITEM,
      .prim_key = item
   };
   return k.key;
}


/*
 * TPCH loader
 * Generate all the elements that will be stored in the DB
 */

/* 'WAREHOUSE' elements */
static char *tpch_lineitem(uint64_t uid) {
   uint64_t key = get_key_lineitem(uid);
   char *item = create_shash(key);
   item = add_shash_string(item, LINENUMBER, 255, 255, 1);
   item = add_shash_string(item, QUANTITY, 255, 255, 1);
   item = add_shash_string(item, DISCOUNT, 255, 255, 1);
   return item;
}

/*
 * Function called by the workload-common interface. uid goes from 0 to the number of elements in the DB.
 */
static char *create_unique_item_tpch(uint64_t uid, uint64_t max_uid) {
   if(uid == 0)
      assert(max_uid == get_db_size_tpch()); // set .nb_items_in_db = get_db_size_tpch() in main.c otherwise the DB will be partially populated

   if(uid < NB_LINEITEMS)
      return tpch_lineitem(uid);
   uid -= NB_LINEITEMS;

   return NULL;
}


size_t get_db_size_tpch(void) {
   return NB_LINEITEMS
      ;
}


/*
 * Generic callback helpers. Allow to automatically perform the multiple steps of a TPCH query without dealing too much with asynchrony.
 */
static volatile uint64_t failed_trans, nb_commits_done;
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

/*
 * Q1.sql
 */
void q1_map(struct slab_callback *cb, void *item) {
   if(!item)
      __sync_fetch_and_add(&nb_commits_done, 1);
}

void q1(struct injector_queue *q) {
   //kv_long_scan(q, get_key_lineitem(0), get_key_lineitem(NB_LINEITEMS), q1_map);
}


/*
 * Q2.sql
 */
struct q2_payload {
    uint64_t completed_requests;
};

void _q2_1(struct slab_callback *cb, void *item) {
   struct slab_callback *new_cb = new_slab_callback();
   new_cb->cb = commit_cb;
   new_cb->transaction = cb->transaction;
   new_cb->injector_queue = cb->injector_queue;
   kv_commit(cb->transaction, new_cb);

   free(cb->item);
   free(cb);
}

void q2(struct injector_queue *q) {
   struct q2_payload *p = calloc(1, sizeof(*p));

   /* Create the long transaction scan */
   struct transaction *t = create_transaction();
   long x = uniform_next() % NB_LINEITEMS;

   struct slab_callback *cb = new_slab_callback();
   cb->cb = _q2_1;
   cb->injector_queue = q;
   cb->item = tpch_lineitem(x);
   kv_trans_write(t, cb);
}



static void _launch_tpch(struct workload *w, int test, int nb_requests, int zipfian) {
   struct injector_queue *q = create_new_injector_queue();

   //create_transaction(); // screw up the snapshots!

   declare_periodic_count;
   for(size_t i = 0; i < nb_requests; i++) {
      //long x = uniform_next() % 100;
      if(i == 0) {
         q1(q);
      } else {
         q2(q);
      }
      injector_process_queue(q);
      periodic_count(1000, "TRANS Load Injector (%lu%%) [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu]", i*100LU/(nb_requests?nb_requests:1), failed_trans*100/(nb_commits_done?nb_commits_done:1), get_snapshot_size(), get_nb_running_transactions());
   }

   do {
      injector_process_queue(q);
   } while(nb_commits_done != nb_requests * w->nb_load_injectors);
   //} while(nb_commits_done != 4);
   printf("[%lu requests => %lu IOs]\n", w->nb_requests, w->nb_requests*95*3/100+w->nb_requests*NB_LINEITEMS);
}

/* Generic interface */
static void launch_tpch(struct workload *w, bench_t b) {
   return _launch_tpch(w, 0, w->nb_requests_per_thread, 0);
}

/* Pretty printing */
static const char *name_tpch(bench_t w) {
   printf("TPCH BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu] [map calls %lu]\n", ((double)failed_trans)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size(), 0LU);
   return "TPCH";
}

static int handles_tpch(bench_t w) {
   failed_trans = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;

   switch(w) {
      case tpch:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_tpch(void) {
   return "TPCH";
}

struct workload_api TPCH = {
   .handles = handles_tpch,
   .launch = launch_tpch,
   .api_name = api_name_tpch,
   .name = name_tpch,
   .create_unique_item = create_unique_item_tpch,
};
