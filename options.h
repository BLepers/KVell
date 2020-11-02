#ifndef OPTIONS_H
#define OPTIONS_H

#define DEBUG 0
#define PINNING 0
#define SPINNING 0


#define TRANSACTION_OBJECT_SIZE 512
#define PATH "/scratch%lu/kvell/slab-%d-%lu"   // path where data is store -- disk, worker_id, item_size
#define PATH_TRANSACTIONS "/scratch%lu/kvell/trans-%d-%lu" // path where the transaction log is store -- disk, worker_id, transaction_size

/* Which transaction type are we using? */
#define TRANS_SNAPSHOT 0
#define TRANS_FAST 1
#define TRANS_LONG 2
#define TRANSACTION_TYPE TRANS_LONG
/*#define TRANSACTION_TYPE TRANS_SNAPSHOT*/

/* Queue depth management */
#define QUEUE_DEPTH 32
#define MAX_NB_PENDING_CALLBACKS_PER_WORKER (2*QUEUE_DEPTH)
#define NEVER_EXCEED_QUEUE_DEPTH 0 // Never submit more than QUEUE_DEPTH IO requests simultaneously, otherwise up to 2*MAX_NB_PENDING_CALLBACKS_PER_WORKER (very unlikely)

/* Snapshot management */
#if TRANSACTION_TYPE == TRANS_SNAPSHOT
/*#define MAX_CLEANING_OP_PER_ROUND 100*/
#define MAX_CLEANING_OP_PER_ROUND 1000000000
#else
#define MAX_CLEANING_OP_PER_ROUND 10000000
#endif

/* Page cache */
/*#define PAGE_CACHE_SIZE (PAGE_SIZE * 20480)*/
/*#define PAGE_CACHE_SIZE (PAGE_SIZE * 7864320) //30GB*/
#define PAGE_CACHE_SIZE (PAGE_SIZE * 5242880) //20GB
//#define PAGE_CACHE_SIZE (PAGE_SIZE * 786432) //3GB
#define MAX_PAGE_CACHE (PAGE_CACHE_SIZE / PAGE_SIZE)

/* Injector queues */
#define SAFE_INJECTOR_QUEUES 1 // see injectorqueue.c

/* Free list */
#define FREELIST_IN_MEMORY_ITEMS (25600) // We need enough to never have to read from disk

/* BATCHING of requests for scans */
#define MAX_BATCH_SIZE 30
#endif
