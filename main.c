#include "headers.h"

volatile int _commit_done = 0;
void commit_done(struct slab_callback *cb, void *item) {
   printf("Just committed transaction %lu\n", get_transaction_id(cb->transaction));
   _commit_done = 1;
   free(cb->transaction);
   free(cb);
}

int commit(struct transaction *t, struct injector_queue *q) {
   struct slab_callback *new_cb = new_slab_callback();
   new_cb->cb = commit_done;
   new_cb->transaction = t;
   new_cb->injector_queue = q;
   _commit_done = 0;
   return kv_commit(t, new_cb);
}

void cb_trans(struct slab_callback *cb, void *item) {
   if(item) {
      printf("\t[%lu] Just read an item (status: %s): ", get_transaction_id(cb->transaction), has_failed(cb->transaction)?"failed":"success");
      print_item(cb->action, item);
   } else {
      printf("\t[%lu] Just wrote an item (status: %s): ", get_transaction_id(cb->transaction), has_failed(cb->transaction)?"failed":"success");
      print_item(cb->action, cb->item);
   }

   free(cb->item);
   free(cb);
}

struct slab_callback *t_bench_cb(void) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = cb_trans;
   return cb;
}

int main(int argc, char **argv) {
   int nb_disks, nb_workers_per_disk;
   declare_timer;

   /* Definition of the workload, if changed you need to erase the DB before relaunching */
   struct workload w = {
      //.api = &TPCC,
      //.api = &TPCCH,
      .api = &SCAN,
      //.api = &PRODUCTION,
      //.nb_items_in_db = get_db_size_tpcch(),
      .nb_items_in_db = 100000000LU,
      //.nb_items_in_db = 5000000000LU,
      //.nb_items_in_db = 1000000LU,
      //.nb_items_in_db = 25000000LU,
      //.nb_items_in_db = 10LU,
      //.nb_items_in_db = get_db_size_tpch(),
      .nb_load_injectors = 4,
      //.nb_load_injectors = 1,
      //.nb_load_injectors = 12, // For scans (see scripts/run-aws.sh and OVERVIEW.md)
   };


   /* Parsing of the options */
   if(argc < 3)
      die("Usage: ./main <nb disks> <nb workers per disk>\n\tData is stored in %s\n", PATH);
   nb_disks = atoi(argv[1]);
   nb_workers_per_disk = atoi(argv[2]);

   /* Pretty printing useful info */
   printf("# Configuration:\n");
   printf("# \tPage cache size: %lu GB\n", PAGE_CACHE_SIZE/1024/1024/1024);
   printf("# \tWorkers: %d working on %d disks\n", nb_disks*nb_workers_per_disk, nb_disks);
   printf("# \tIO configuration: %d queue depth (capped: %s)\n", QUEUE_DEPTH, NEVER_EXCEED_QUEUE_DEPTH?"yes":"no");
   printf("# \tQueue configuration: %d maximum pending callbaks per worker\n", MAX_NB_PENDING_CALLBACKS_PER_WORKER);
   printf("# \tThread pinning: %s spinning: %s\n", PINNING?"yes":"no", SPINNING?"yes":"no");
   printf("# \tBench: %s (%lu elements)\n", w.api->api_name(), w.nb_items_in_db);
   printf("# \tSnapshot: %s\n", TRANSACTION_TYPE==TRANS_LONG?"BSI":"SI");

   /* Initialization of random library */
   start_timer {
      init_transaction_manager();
      printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
      init_zipf_generator(0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but this is legacy code :/ */
   } stop_timer("Initializing random number generator (Zipf)");

   /* Recover database */
   start_timer {
      slab_workers_init(nb_disks, nb_workers_per_disk);
   } stop_timer("Init found %lu elements", get_database_size());

   /* Add missing items if any */
   repopulate_db(&w);
   while(pending_work()); // Wait for the DB to be fully populated

   usleep(500000);


   /* Launch benchs */
   bench_t workload, workloads[] = {
      scan_d_uniform,
      //scan_a_uniform,
      //scan_b_uniform,
      //
      //scan_d_zipfian,
      //scan_a_zipfian,
      //scan_b_zipfian,
      //tpcch,
      //prod1,
      //prod2,
   };
   //int nb_scan, nb_scans[] = { 4, 0, 1, 2 };
   //int nb_scan, nb_scans[] = { 1, 2, 4, 8, 16, 32 };
   int nb_scan, nb_scans[] = { 1};
   foreach(workload, workloads) {
      foreach(nb_scan, nb_scans) {
         w.nb_requests = 20000000LU;
         //w.nb_requests = 120000000LU;
         w.nb_scans = nb_scan;
         //w.nb_requests = 450000000LU;
         //w.nb_requests = 10000LU;
         printf("LAUNCHING WITH %d SCANS\n", nb_scan);
         run_workload(&w, workload);
      }
   }

   return 0;

   /*struct injector_queue *q = create_new_injector_queue();
   struct transaction *t1, *t2, *t3, *t4, *t5;

   {
      struct slab_callback *map_cb = new_slab_callback();
      map_cb->cb = cb_trans_scan_map;
      map_cb->injector_queue = q;
      t1 = create_long_transaction(map_cb, NULL);
      printf("Creating t1 snapshot id %lu\n", get_snapshot_version(t1));
   }

   {
      struct slab_callback *map_cb = new_slab_callback();
      map_cb->cb = cb_trans_scan_map;
      map_cb->injector_queue = q;
      t2 = create_long_transaction(map_cb, NULL);
      printf("Creating t2 snapshot id %lu\n", get_snapshot_version(t2));
   }

   {
      struct slab_callback *cb = t_bench_cb();
      cb->injector_queue = q;
      cb->item = create_unique_item(1024, 3);
      kv_trans_write(t2, cb);
      _commit_done = 0;
      commit(t2, q);
      do {
         injector_process_queue(q);
      } while(!_commit_done);
   }


   {
      struct slab_callback *map_cb = new_slab_callback();
      map_cb->cb = cb_trans_scan_map;
      map_cb->injector_queue = q;
      t3 = create_long_transaction(map_cb, NULL);
      printf("Creating t3 snapshot id %lu\n", get_snapshot_version(t3));
   }

   {
      struct slab_callback *cb = t_bench_cb();
      cb->injector_queue = q;
      cb->item = create_unique_item(512, 3);
      kv_trans_write(t3, cb);
      _commit_done = 0;
      commit(t3, q);
      do {
         injector_process_queue(q);
      } while(!_commit_done);
   }

   {
      struct slab_callback *map_cb = new_slab_callback();
      map_cb->cb = cb_trans_scan_map;
      map_cb->injector_queue = q;
      t4 = create_long_transaction(map_cb, NULL);
      printf("Creating t4 snapshot id %lu\n", get_snapshot_version(t4));
   }

   {
      struct slab_callback *cb = t_bench_cb();
      cb->injector_queue = q;
      cb->item = create_unique_item(256, 3);
      kv_trans_write(t4, cb);
      _commit_done = 0;
      commit(t4, q);
      do {
         injector_process_queue(q);
      } while(!_commit_done);
   }

   {
      struct slab_callback *map_cb = new_slab_callback();
      map_cb->cb = cb_trans_scan_map;
      map_cb->injector_queue = q;
      t5 = create_long_transaction(map_cb, NULL);
      printf("Creating t5 snapshot id %lu\n", get_snapshot_version(t5));
   }

   {
      struct slab_callback *cb = t_bench_cb();
      cb->injector_queue = q;
      cb->item = create_unique_item(512, 2);
      kv_trans_write(t5, cb);
      _commit_done = 0;
      commit(t5, q);
      do {
         injector_process_queue(q);
      } while(!_commit_done);
   }


   {
      struct slab_callback *cb = t_bench_scan_cb();
      cb->injector_queue = q;
      cb->item = create_unique_item(1024, 1);
      _commit_done = 0;
      kv_trans_long_start_scan(t1, cb);
      do {
         injector_process_queue(q);
      } while(_commit_done != get_nb_workers());
   }


   commit(t1, q);

   sleep(1);*/

   return 0;
}
