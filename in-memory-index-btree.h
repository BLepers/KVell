#ifndef IN_MEMORY_BTREE
#define IN_MEMORY_BTREE 1

#include "indexes/btree.h"

#define INDEX_TYPE "btree"
#define memory_index_init btree_init
#define memory_index_add btree_index_add
#define memory_index_lookup btree_worker_lookup
#define memory_index_delete btree_worker_delete
#define memory_index_scan btree_init_scan

void btree_init(void);
struct index_entry *btree_worker_lookup(int worker_id, void *item);
void btree_worker_delete(int worker_id, void *item);
struct index_scan btree_init_scan(void *item, size_t scan_size);
void btree_index_add(struct slab_callback *cb, void *item);

#endif

