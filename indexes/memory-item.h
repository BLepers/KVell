#ifndef MEM_ITEM_H
#define MEM_ITEM_H

struct slab;
struct index_entry { // This index entry could be made much smaller by, e.g., have 64b for [slab_size, slab_idx] it is then easy to do size -> slab* given a slab context
   union {
      struct slab *slab;
      void *page;
   };
   union {
      size_t slab_idx;
      void *lru;
   };
};

struct index_scan {
   uint64_t *hashes;
   struct index_entry *entries;
   size_t nb_entries;
};

typedef struct index_entry index_entry_t;


#endif
