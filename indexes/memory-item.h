#ifndef MEM_ITEM_H
#define MEM_ITEM_H

struct slab;

/*
 * Because we are in C and have no template... We use  definition for multiple contexts.
 * Context1: memory index. prefix(key) -> slab, slab_idx
 * Context2: pagecache.  hash(page) -> page, lru
 * Context3: transaction. prefix(key) -> index in the cache of the transaction, flags
 * Context4: MVCC. prefix(key) -> MVCC block -> [ slab, slab_idx ]
 */
struct index_entry { // This index entry could be made much smaller by, e.g., have 64b for [slab_size, slab_idx] it is then easy to do size -> slab* given a slab context
   union {
      struct slab *slab;
      void *page;
      size_t cached_data_idx;
      uint64_t current_rdt;
   };
   union {
      size_t slab_idx;
      void *lru;
      size_t transaction_flags;
      struct index_entry *versions;
   };
   union {
      size_t rdt;
      size_t nb_versions;
   };
};

/*
 * Different flags on the rdt field.
 */
#define ITEM_IS_LOCKED_BIT (1LU<<61LU)
#define ITEM_CONTAINS_NEW_IDX_BIT (1LU<<60LU)

#define TRANSACTION_MASK ((1LU<<60LU)-1LU)

#define get_rdt_value(item) ((item)->rdt & TRANSACTION_MASK)

#define get_new_value_bit(item) ((item)->rdt & ITEM_CONTAINS_NEW_IDX_BIT)
#define set_new_value_bit(item) ((item)->rdt |= ITEM_CONTAINS_NEW_IDX_BIT)
#define unset_new_value_bit(item) ((item)->rdt &= ~(ITEM_CONTAINS_NEW_IDX_BIT))

#define get_locked_bit(item) ((item)->rdt & ITEM_IS_LOCKED_BIT)
#define set_locked_bit(item) ((item)->rdt |= ITEM_IS_LOCKED_BIT)
#define unset_locked_bit(item) ((item)->rdt &= ~(ITEM_IS_LOCKED_BIT))

struct index_scan {
   uint64_t *hashes;
   struct index_entry *entries;
   size_t nb_entries;
};

typedef struct index_entry index_entry_t;


#endif
