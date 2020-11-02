#ifndef IN_MEMORY_memory_index
#define IN_MEMORY_memory_index 1

#include "indexes/btree.h"


void memory_index_init(void);

void memory_index_add(struct slab_callback *cb, void *item);                     // Add an item
void memory_index_reserve(int worker_id, void *item, uint64_t transaction_id);   // Say the item will be added by a transaction
void memory_index_delete(int worker_id, void *item);


index_entry_t *memory_index_lookup(int worker_id, struct slab_callback *cb, void *item, uint64_t transaction_id, int *action_allowed);
index_entry_t *memory_index_lookup_and_lock(int worker_id, void *item, struct slab_callback *cb, uint64_t transaction_id, int *present, int *action_allowed);
index_entry_t *memory_index_lookup_next(int worker_id, struct slab_callback *cb, void *item, uint64_t *found_hash, uint64_t transaction_id, int *action_allowed);
index_entry_t *memory_index_read_next_batch(int worker_id, struct slab_callback *cb, void *item, uint64_t transaction_id, size_t *batch_size, uint64_t **hashes, int *action_allowed);

uint64_t memory_item_update_remove_lock(struct slab_callback *cb, void *item);
void memory_index_clean_old_versions(int worker_id, uint64_t hash, uint64_t snapshot_id);
void memory_index_clean_specific_version(void *item);
void memory_index_revert(int worker_id, void *item, struct slab_callback *cb, uint64_t transaction_id);

size_t get_snapshot_size(void);
#endif

