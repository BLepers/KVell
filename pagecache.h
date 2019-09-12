#ifndef PAGE_CACHE_H
#define PAGE_CACHE_H 1

typedef struct index_entry pagecache_entry_t;

#if PAGECACHE_INDEX == RBTREE

#include "indexes/rbtree.h"
typedef rbtree hash_t;
#define tree_create() rbtree_create()
#define tree_lookup(h, hash) rbtree_lookup((h), (void*)(hash), pointer_cmp)
#define tree_delete(h, hash, old_entry)  rbtree_delete((h), (void*)(hash), pointer_cmp);
#define tree_insert(h, hash, old_entry, dst, lru_entry) \
   do { \
      pagecache_entry_t new_entry = { .page = dst, .lru = lru_entry }; \
      rbtree_insert((h), (void*)(hash), &new_entry, pointer_cmp); \
   } while(0)

#elif PAGECACHE_INDEX == RAX

#include "indexes/rax.h"
typedef rax* hash_t;
#define tree_create() raxNew()
#define tree_lookup(h, hash) ({ void *__v = raxFind((h), (unsigned char*)&(hash), sizeof(hash)); __v==raxNotFound?NULL:__v; })
#define tree_delete(h, hash, old_entry) raxRemove((h), (unsigned char *)&(hash), sizeof(hash), (void**)(old_entry))
#define tree_insert(h, hash, old_entry, dst, lru_entry) \
   do { \
      pagecache_entry_t *new_entry = old_entry; \
      if(!new_entry) \
         new_entry = malloc(sizeof(*new_entry)); \
      new_entry->page = dst; \
      new_entry->lru = lru_entry; \
      raxInsert((h),(unsigned char*)&(hash),sizeof(hash),new_entry,NULL); \
   } while(0)

#elif PAGECACHE_INDEX == ART

#include "indexes/art.h"
typedef art_tree* hash_t;
#define tree_create() ({ art_tree *___t = malloc(sizeof(*___t)); art_tree_init(___t); ___t; })
#define tree_lookup(h, hash) art_search((h), (unsigned char*)&(hash), sizeof(hash))
#define tree_delete(h, hash, old_entry) *old_entry = art_delete((h), (unsigned char *)&(hash), sizeof(hash))
#define tree_insert(h, hash, old_entry, dst, lru_entry) \
   do { \
      pagecache_entry_t *new_entry = old_entry; \
      if(!new_entry) \
         new_entry = malloc(sizeof(*new_entry)); \
      new_entry->page = dst; \
      new_entry->lru = lru_entry; \
      art_insert((h),(unsigned char*)&(hash),sizeof(hash),new_entry); \
   } while(0)

#elif PAGECACHE_INDEX == BTREE

#include "indexes/btree.h"
typedef btree_t* hash_t;
#define tree_create() btree_create()
#define tree_lookup(h, hash) ({ int res = btree_find((h), (unsigned char*)&(hash), sizeof(hash), &tmp_entry); res?&tmp_entry:NULL; })
#define tree_delete(h, hash, old_entry) \
   do { \
      btree_delete((h), (unsigned char*)&(hash), sizeof(hash)); \
   } while(0);
#define tree_insert(h, hash, old_entry, dst, lru_entry) \
   do { \
      pagecache_entry_t new_entry = { .page = dst, .lru = lru_entry }; \
      btree_insert((h),(unsigned char*)&(hash),sizeof(hash), &new_entry); \
   } while(0)


#endif


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
