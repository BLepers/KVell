# OVERVIEW

## Warning note
KVell is a prototype. Many things have not been tested. Use it as an inspiration, don't use it in production.

## Logic and path of a query

KVell has 2 distinct types of threads. Load injector threads, and worker threads.

* Requests are sent by the  load injector threads. Load injector threads basically:
  * Create a request `struct slab_callback *cb`. A request contains an `item = { key, value }` and a callback that is called when the request has been processed.
  * Enqueue the request in the KV, using one of the `kv_xxx` function (e.g., `kv_read_async(cb)` ). Requests partionned amongst workers based on the prefix of the key modulo number of workers.
  * Main code to generate the callbacks is in [workload-ycsb.c](workload-ycsb.c) and the enqueue code is in [slabworker.c](slabworker.c).
  * For scans, the load injector threads also merges keys from all the in-memory indexes of worker threads. See `btree_init_scan` for instance.

* Workers threads do the actual work. In a big loop (`worker_slab_init`):
  * They dequeue requests and figure out from which file queried item should be read or written (`worker_dequeue_requests` [slabworker.c](slabworker.c))
    * Which call functions that compute where the item is in the file (e.g., `read_item_async` [slab.c](slab.c))
       * The location of existing items is store in in-memory indexes (e.g., `btree_worker_lookup` [in-memory-index-btree.c](in-memory-index-btree.c))
       * Which call functions that check if the item is cached or if an IO request should be created (e.g., `read_page_async` [ioengine.c](ioengine.c))
  * After dequeueing enough requests, or when the IO queue is full, or when no request can be dequeued anymore, then IOs are sent to disk (`worker_ioengine_enqueue_ios` [slabworker.c](slabworker.c))
  * We then wait for the disk to process IOs (`worker_ioengine_get_completed_ios`)
  * And finally we call the callbacks of all processed requests (`worker_ioengine_process_completed_ios`)

## Options

[options.h](main.c) contains main configuration options.

Mainly, you want to configure `PATH` to point to a directory that exists.
```c
#define PATH "/scratch%lu/kvell/slab-%d-%lu-%lu"
#define PATH "/scratch[disk_id]/kvell/slab-[workerid]-[legacy, always 0]-[itemsize]"
```

You probably want to disable `PINNNING`, unless you use less threads than cores.

And on small machines, you should reduce `PAGE_CACHE_SIZE`.


## Workload parameters

[main.c](main.c) contains workloads parameters.

```c
struct workload w = {
   .api = &YCSB,
   .nb_items_in_db = 100000000LU, // Size of the DB, if you change that, you must delete the DB before rerunning a benchmark
   .nb_load_injectors = 4,
};
```

## Launch a bench
```bash
./main <number of disks> <number of workers per disk>
e.g. ./main 8 4 # will use a total of 32 workers + 4 load injectors if using the workload definition above = 36 threads in total
```

## Good to know
* Because the database is statically partitionned, if you change the number of workers (`./main 1 2` vs. `./main 1 3` for instance), you must delete the database first. This could be avoided by rebuilding the database on startup, but this is not implemented.
* Items larger than 4K are currently not handled by the DB (this would be rather trivial to add in [slab.c](slab.c) by issuing multiple read or write queries, but this is not implemented currently).
* Because the merging of indexes is done by injector threads and not worker threads, workloads that mainly perform scans benefit from having way more injectors than workers. In the future we might change the logic so that the merging is done by workers, this would make more sense and would probably be faster. This is the reason why the benchmark script for AWS uses two different configurations for YCSB[ABC] and YCSB[E].

## Common errors
If you get this error then the page cache doesn't fit in memory:
```c
main: pagecache.c:27: void page_cache_init(struct pagecache *): Assertion `p->cached_data' failed.
```
In general if you get errors, try to run with a smaller DB, it's probably because the indexes do not fit in RAM.
