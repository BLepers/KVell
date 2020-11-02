#ifndef PAGE_CACHE_H
#define PAGE_CACHE_H 1

typedef struct index_entry pagecache_entry_t;


#include "indexes/btree.h"
typedef btree_t* hash_t;
#define tree_create() btree_create()
#define tree_lookup(h, hash) ({ int res = btree_find((h), (unsigned char*)&(hash), sizeof(hash), &tmp_entry); res?tmp_entry:NULL; })
#define tree_delete(h, hash, old_entry) \
   do { \
      btree_delete((h), (unsigned char*)&(hash), sizeof(hash)); \
   } while(0);
#define tree_insert(h, hash, old_entry, dst, lru_entry) \
   do { \
      pagecache_entry_t new_entry = { .page = dst, .lru = lru_entry }; \
      btree_insert((h),(unsigned char*)&(hash),sizeof(hash), &new_entry); \
   } while(0)


struct lru {
   struct lru *prev;
   struct lru *next;
   uint64_t hash;
   void *page;
   int contains_data;
   int dirty;
};

struct pagecache {
   char *cached_data;
   hash_t hash_to_page;
   struct lru *used_pages, *oldest_page, *newest_page;
   size_t used_page_size;
};

void page_cache_init(struct pagecache *p);
int get_page(struct pagecache *p, uint64_t hash, void **page, struct lru **lru);

#endif
