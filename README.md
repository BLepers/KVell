# KVell+: Snapshot Isolation Without Snapshots


## Compiling

```bash
# On Ubuntu Server 18.04 LTS
sudo apt install make clang autoconf libtool

# Install gpertools (optionnal, but KVell will be slower without TCMalloc):
cd ~
git clone https://github.com/gperftools/gperftools.git
cd gpertools
./autogen.sh
./configure
make -j 36

# Install KVell
cd ~
git clone https://github.com/BLepers/KVell
cd KVell
make -j
```

## Running

* Edit options.h to set the path of the DB and the pagecache size.
```C
#define TRANS_SNAPSHOT 0      // Run with vanilla snapshot isolation
#define TRANS_FAST 1          // Run with transactions but not snapshot (transactions abort when they try to read an item that is no longer there)
#define TRANS_LONG 2          // Run with OLCP
#define TRANSACTION_TYPE TRANS_LONG // By default OLCP is active
```

* Edit main.c to set the workload:

```C
struct workload w = {
   .api = &SCAN, // YCSB-T, see workload-common.h for different workloads
   .nb_items_in_db = 1000LU, // number of items; must be get_db_size_tpch() for TPC-CH
   .nb_load_injectors = 1, // number of injector threads
};

bench_t workload, workloads[] = {
   scan_d_uniform, // see workload-common.h for benchmarks; make sure the benchmark runs on the correct api (e.g., scan_xxx requires .api = &SCAN)
};
```

```bash
make -j
./main <number of disks> <number of workers per disk>
   E.g.: ./main 1 1
```

## Major differences with the paper

* The paper presents an "olcp\_query" interface, but the implementation uses the asynchronous API of KVell. To launch an OLCP query, you need to do:

```C
static void map(struct slab_callback *cb, void *item) {
   if(!item) {
      // End of the scan
   } else {
      // process item
   }
}

/* Scan range ]10,15] */
struct injector_queue *q = create_new_injector_queue();
struct slab_callback *scan_cb = new_slab_callback();
scan_cb->cb = map;
scan_cb->injector_queue = q;
scan_cb->item = create_key(10);
scan_cb->max_next_key = 15;
kv_long_scan(scan_cb); // Start a background scan; the function is non blocking.
```

Injector queues are used to push responses from a worker to an injector (as described in the paper). Enqueued items are processed when the "injector\_process\_queue" function is called. A typical way of doing it is to add the following after the start of the background scan:

```C
while(!scan_complete) { // add code in map to set scan_complete to 1 when the map function is called with item == NULL
   injector_process_queue(q);
   // Feel free to add a sleeping or blocking wait mechanism here
}
```


* OLCP queries are safe when executed with concurrent transactions, but probably not safe when executed with concurrent queries executed outside of a transaction. To run a concurrent query in a transaction:

```C
static void commit_done(struct slab_callback *cb, void *item) {
   if(has_failed(cb->transaction))
      // handle failure
   free(cb->transaction);
   free(cb);
}

static void write_done(struct slab_callback *cb, void *item) {
   // write is done but not persisted; commit the transaction to persist
   struct slab_callback *new_cb = new_slab_callback();
   new_cb->cb = commit_done;
   new_cb->transaction = cb->transaction;
   new_cb->injector_queue = cb->injector_queue;
   kv_commit(cb->transaction, new_cb);

   free(cb->item);
   free(cb);
}

struct transaction *t = create_transaction();
struct slab_callback *cb = new_slab_callback();
cb->injector_queue = q;
cb->cb = write_done;
cb->item = create_unique_item_with_value(1024, 15, 42); // 1KB item with key 15 and value 42
kv_trans_write(t, cb);

// Don't forget to process the injector queue, otherwise "write_done" and "commit_done" will never be called
```

See workload-scan.c for a full example.

* "Point ranges" are not implemented. Currently, OLCP queries can only have 1 scan range. It implies that when OLCP is active, no item is versionned to the length of the long transactions. To modify, edit the garbage collector (gc.c):
```C
/* Do something smarter here: */
if(TRANSACTION_TYPE == TRANS_LONG)
   snapshot_id = get_min_in_commit(); // aggressive cleaning with OLCP
else
   snapshot_id = get_min_snapshot_id(); // keep versions for vanilla SI
```

## Minor remarks

* The code of TPC-CH is in a rough state. Crashes might happen.

* The paper only mentions 1 memory index but, in the code, the current versions are kept in an index and the old versions are kept in another memory index.

* The paper uses t\_snapshot = min\_in\_commit - 1, but the code just uses min\_in\_commit. So a transaction does not read items whose timestamp is equal to its t\_snapshot; it only reads items that are strictly more recent. (Algorithms are the same, just replace all <= in the paper with <.)

* It is quite possible that the recovery-in-case-of-a-crash code does not fully work.

* Just as in the original KVell, items of more than 4KB are not supported.

* It is possible that the first workload executed with vanilla snapshot isolation runs extremely fast. It is because new values are appended sequentially during the first run. The second run will reuse the free spots and should run at a normal speed.

* The code has changed since the submission, so it is possible that performance does not exactly match what we observed in the paper. Hopefully, differences should be minimal. In general, the code is not heavily optimized for performance, so it does not constitue an extremly good baseline to improve upon.

* In general, consider the code as a prototype/proof of concept. The code might have unhandled data races and various bugs.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Papers

Baptiste Lepers, Oana Balmau, Karan Gupta, and Willy Zwaenepoel. 2019. KVell: the Design and Implementation of a Fast Persistent Key-Value Store. In Proceedings of SOSP’19: ACM Symposium on Operating Systems Principles (SOSP’19) [[pdf](sosp19-final40.pdf)].

Baptiste Lepers, Oana Balmau, Karan Gupta, and Willy Zwaenepoel. 2021. KVell+: Snapshot Isolation Without Snapshots. In Proceedings of OSDI’20: USENIX Symposium on Operating Systems Design and Implementation (OSDI’20) [[pdf](osdi20-final180.pdf)].
