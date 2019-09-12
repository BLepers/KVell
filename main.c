#include "headers.h"

int main(int argc, char **argv) {
   int nb_disks, nb_workers_per_disk;
   declare_timer;

   /* Definition of the workload, if changed you need to erase the DB before relaunching */
   struct workload w = {
      .api = &YCSB,
      .nb_items_in_db = 100000000LU,
      .nb_load_injectors = 4,
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
   printf("# \tIO configuration: %d queue depth (capped: %s, extra waiting: %s)\n", QUEUE_DEPTH, NEVER_EXCEED_QUEUE_DEPTH?"yes":"no", WAIT_A_BIT_FOR_MORE_IOS?"yes":"no");
   printf("# \tQueue configuration: %d maximum pending callbaks per worker\n", MAX_NB_PENDING_CALLBACKS_PER_WORKER);
   printf("# \tDatastructures: %d (memory index) %d (pagecache)\n", MEMORY_INDEX, PAGECACHE_INDEX);
   printf("# \tThread pinning: %s\n", PINNING?"yes":"no");
   printf("# \tBench: %s (%lu elements)\n", w.api->api_name(), w.nb_items_in_db);

   /* Initialization of random library */
   start_timer {
      printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
      init_zipf_generator(0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but this is legacy code :/ */
   } stop_timer("Initializing random number generator (Zipf)");

   /* Recover database */
   start_timer {
      slab_workers_init(nb_disks, nb_workers_per_disk);
   } stop_timer("Init found %lu elements", get_database_size());

   /* Add missing items if any */
   repopulate_db(&w);

   /* Launch benchs */
   bench_t workload, workloads[] = {
      ycsb_a_uniform, ycsb_b_uniform, ycsb_c_uniform,
      ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,
      //ycsb_e_uniform, ycsb_e_zipfian, // Scans
   };
   foreach(workload, workloads) {
      if(workload == ycsb_e_uniform || workload == ycsb_e_zipfian) {
         w.nb_requests = 2000000LU; // requests for YCSB E are longer (scans) so we do less
      } else {
         w.nb_requests = 100000000LU;
      }
      run_workload(&w, workload);
   }
   return 0;
}
