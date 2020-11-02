#include "headers.h"

/*
 * Create a workload item for the database
 */
char *create_unique_item(size_t item_size, uint64_t uid) {
   char *item = malloc(item_size);
   struct item_metadata *meta = (struct item_metadata *)item;
   meta->key_size = 8;
   meta->value_size = item_size - 8 - sizeof(*meta);

   char *item_key = &item[sizeof(*meta)];
   char *item_value = &item[sizeof(*meta) + meta->key_size];
   *(uint64_t*)item_key = uid;
   *(uint64_t*)item_value = uid;
   return item;
}

/* We also store an item in the database that says if the database has been populated for YCSB, PRODUCTION, or another workload. */
char *create_workload_item(struct workload *w) {
   const uint64_t key = -10;
   const char *name = w->api->api_name(); // YCSB or PRODUCTION?
   size_t key_size = 16;
   size_t value_size = strlen(name) + 1;

   struct item_metadata *meta;
   char *item = malloc(sizeof(*meta) + key_size + value_size);
   meta = (struct item_metadata *)item;
   meta->key_size = key_size;
   meta->value_size = value_size;

   char *item_key = &item[sizeof(*meta)];
   char *item_value = &item[sizeof(*meta) + meta->key_size];
   *(uint64_t*)item_key = key;
   strcpy(item_value, name);
   return item;
}

/*
 * Fill the DB with missing items
 */
static void add_in_tree(struct slab_callback *cb, void *item) {
   memory_index_add(cb, item);
   free(cb->item);
   free(cb);
}

struct rebuild_pdata {
   size_t id;
   size_t *pos;
   size_t start;
   size_t end;
   struct workload *w;
};
void *repopulate_db_worker(void *pdata) {
   declare_periodic_count;
   struct rebuild_pdata *data = pdata;

   pin_me_on(get_nb_workers() + data->id);

   size_t *pos = data->pos;
   struct workload *w = data->w;
   struct workload_api *api = w->api;
   size_t start = data->start;
   size_t end = data->end;
   for(size_t i = start; i < end; i++) {
      struct slab_callback *cb = malloc(sizeof(*cb));
      cb->cb = add_in_tree;
      cb->payload = NULL;
      cb->item = api->create_unique_item(pos[i], w->nb_items_in_db);
      kv_add_async(cb);
      periodic_count(1000, "Repopulating database (%lu%%)", 100LU-(end-i)*100LU/(end - start));
   }

   return NULL;
}

void repopulate_db(struct workload *w) {
   declare_timer;
   void *workload_item = create_workload_item(w);
   int64_t nb_inserts = (get_database_size() > w->nb_items_in_db)?0:(w->nb_items_in_db - get_database_size());


   if(nb_inserts != w->nb_items_in_db) { // Database at least partially populated
      // Check that the items correspond to the workload
      char *db_item = kv_read_sync(workload_item);
      if(!db_item)
         die("Running a benchmark on a pre-populated DB, but couldn't determine if items in the DB correspond to the benchmark --- please wipe DB before benching!\n");
      struct item_metadata *meta = (struct item_metadata *)db_item;
      char *item_value = &db_item[sizeof(*meta) + meta->key_size];
      if(strcmp(w->api->api_name(), item_value))
         die("Running %s benchmark, but the database contains elements from %s benchmark -- please wipe DB before benching!\n", w->api->api_name(), item_value);
   }

   if(nb_inserts == 0) {
      free(workload_item);
      return;
   }

   uint64_t nb_items_already_in_db = get_database_size();

   // Say that this database is for that workload
   if(nb_items_already_in_db == 0) {
      struct slab_callback *cb = malloc(sizeof(*cb));
      cb->cb = add_in_tree;
      cb->payload = NULL;
      cb->item = workload_item;
      kv_add_async(cb);
   } else {
      nb_items_already_in_db--; // do not count the workload_item
   }
   if(nb_items_already_in_db != 0 && nb_items_already_in_db != w->nb_items_in_db) {
      /*
       * Because we shuffle elements, we don't really want to start with a small database and have all the higher order elements at the end, that would be cheating.
       * Plus, we insert database items at random positions (see shuffle below) and I am too lazy to implement the logic of doing the shuffle minus existing elements.
       */
      die("The database contains %lu elements but the benchmark is configured to use %lu. Please delete the DB first.\n", nb_items_already_in_db, w->nb_items_in_db);
   }


   size_t *pos = NULL;
   start_timer {
      printf("Initializing big array to insert elements in random order... This might take a while. (Feel free to comment but then the database will be sorted and scans much faster -- unfair vs other systems)\n");
      pos = malloc(w->nb_items_in_db * sizeof(*pos));
      for(size_t i = 0; i < w->nb_items_in_db; i++)
         pos[i] = i;
      shuffle(pos, nb_inserts); // To be fair to other systems, we shuffle items in the DB so that the DB is not fully sorted by luck
   } stop_timer("Big array of random positions");

   start_timer {
      struct rebuild_pdata *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));
      pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
      for(size_t i = 0; i < w->nb_load_injectors; i++) {
         pdata[i].id = i;
         pdata[i].start = (w->nb_items_in_db / w->nb_load_injectors)*i;
         pdata[i].end = (w->nb_items_in_db / w->nb_load_injectors)*(i+1);
         if(i == w->nb_load_injectors - 1)
            pdata[i].end = w->nb_items_in_db;
         pdata[i].w = w;
         pdata[i].pos = pos;
         if(i)
            pthread_create(&threads[i], NULL, repopulate_db_worker, &pdata[i]);
      }
      repopulate_db_worker(&pdata[0]);
      for(size_t i = 1; i < w->nb_load_injectors; i++)
         pthread_join(threads[i], NULL);
      free(threads);
      free(pdata);
   } stop_timer("Repopulating %lu elements (%lu req/s)", nb_inserts, nb_inserts*1000000/elapsed);

   free(pos);
}

/*
 *  Print an item stored on disk
 */
void print_item(size_t idx, void* _item) {
   char *item = _item;
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   if(meta->key_size == 0)
      printf("[%lu] Non existant?\n", idx);
   else if(meta->key_size == -1)
      printf("[%lu] Removed\n", idx);
   else
      printf("[%lu] K=%lu V=%s\n", idx, *(uint64_t*)item_key, &item[sizeof(*meta) + meta->key_size]);
}

/*
 * Various callbacks that are called once an item has been read / written
 */
void show_item(struct slab_callback *cb, void *item) {
   print_item(cb->slab_idx, item);
   free(cb->item);
   free(cb);
}

void free_callback(struct slab_callback *cb, void *item) {
   free(cb->item);
   free(cb);
}

void compute_stats(struct slab_callback *cb, void *item) {
   uint64_t start, end;
   declare_debug_timer;
   start_debug_timer {
      start = get_time_from_payload(cb, 0);
      rdtscll(end);
      add_timing_stat(end - start);
      if(DEBUG && cycles_to_us(end-start) > 10000) { // request took more than 10ms
         printf("Request [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu]\n",
               get_origin_from_payload(cb, 1), get_time_from_payload(cb, 1) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 1) - start),
               get_origin_from_payload(cb, 2), get_time_from_payload(cb, 2) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 2) - start),
               get_origin_from_payload(cb, 3), get_time_from_payload(cb, 3) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 3) - start),
               get_origin_from_payload(cb, 4), get_time_from_payload(cb, 4) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 4) - start),
               get_origin_from_payload(cb, 5), get_time_from_payload(cb, 5) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 5) - start),
               get_origin_from_payload(cb, 6), get_time_from_payload(cb, 6) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 6) - start),
               get_origin_from_payload(cb, 7), get_time_from_payload(cb, 7) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 7) - start),
               cycles_to_us(end  - start));
      }
      free(cb->item);
      if(DEBUG)
         free_payload(cb);
      free(cb);
   } stop_debug_timer(5000, "Callback took more than 5ms???");
}

struct slab_callback *bench_cb(void) {
   struct slab_callback *cb = malloc(sizeof(*cb));
   cb->cb = compute_stats;
   cb->payload = allocate_payload();
   return cb;
}


/*
 * Generic worklad API.
 */
struct thread_data {
   size_t id;
   struct workload *workload;
   bench_t benchmark;
};

struct workload_api *get_api(bench_t b) {
   if(YCSB.handles(b))
      return &YCSB;
   if(PRODUCTION.handles(b))
      return &PRODUCTION;
   die("Unknown workload for benchmark!\n");
}

static pthread_barrier_t barrier;
void* do_workload_thread(void *pdata) {
   struct thread_data *d = pdata;

   init_seed();
   pin_me_on(get_nb_workers() + d->id);
   pthread_barrier_wait(&barrier);

   d->workload->api->launch(d->workload, d->benchmark);

   return NULL;
}

void run_workload(struct workload *w, bench_t b) {
   struct thread_data *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));

   w->nb_requests_per_thread = w->nb_requests / w->nb_load_injectors;
   pthread_barrier_init(&barrier, NULL, w->nb_load_injectors);

   if(!w->api->handles(b))
      die("The database has not been configured to run this benchmark! (Are you trying to run a production benchmark on a database configured for YCSB?)");

   declare_timer;
   start_timer {
      pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
      for(int i = 0; i < w->nb_load_injectors; i++) {
         pdata[i].id = i;
         pdata[i].workload = w;
         pdata[i].benchmark = b;
         if(i)
            pthread_create(&threads[i], NULL, do_workload_thread, &pdata[i]);
      }
      do_workload_thread(&pdata[0]);
      for(int i = 1; i < w->nb_load_injectors; i++)
         pthread_join(threads[i], NULL);
      free(threads);
   } stop_timer("%s - %lu requests (%lu req/s)", w->api->name(b), w->nb_requests, w->nb_requests*1000000/elapsed);
   print_stats();

   free(pdata);
}

