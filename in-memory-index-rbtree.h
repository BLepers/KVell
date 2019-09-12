#ifndef IN_MEMORY_RBTREE
#define IN_MEMORY_RBTREE 1

#include "indexes/rbtree.h"

#define INDEX_TYPE "rbtree"
#define memory_index_init rbtree_init
#define memory_index_add rbtree_index_add
#define memory_index_lookup rbtree_worker_lookup
#define memory_index_delete rbtree_worker_delete
#define memory_index_scan rbtree_init_scan

void rbtree_init(void);
struct index_entry *rbtree_worker_lookup(int worker_id, void *item);
void rbtree_worker_delete(int worker_id, void *item);
struct index_scan rbtree_init_scan(void *item, size_t scan_size);
void rbtree_index_add(struct slab_callback *cb, void *item);

#endif

