#ifndef IN_MEMORY_RAX
#define IN_MEMORY_RAX 1

#include "indexes/rax.h"

#define INDEX_TYPE "rax"
#define memory_index_init rax_init
#define memory_index_add rax_index_add
#define memory_index_lookup rax_worker_lookup
#define memory_index_delete rax_worker_delete
#define memory_index_scan rax_init_scan

void rax_init(void);
struct index_entry *rax_worker_lookup(int worker_id, void *item);
void rax_worker_delete(int worker_id, void *item);
struct index_scan rax_init_scan(void *item, size_t scan_size);
void rax_index_add(struct slab_callback *cb, void *item);

#endif
