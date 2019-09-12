#ifndef IN_MEMORY_ART
#define IN_MEMORY_ART 1

#include "indexes/art.h"

#define INDEX_TYPE "art"
#define memory_index_init art_init
#define memory_index_add art_index_add
#define memory_index_lookup art_worker_lookup
#define memory_index_delete art_worker_delete
#define memory_index_scan art_init_scan

void art_init(void);
struct index_entry *art_worker_lookup(int worker_id, void *item);
void art_worker_delete(int worker_id, void *item);
struct index_scan art_init_scan(void *item, size_t scan_size);
void art_index_add(struct slab_callback *cb, void *item);

#endif

