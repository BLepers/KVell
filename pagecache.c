#include "headers.h"

/*
 * Basic page cache implementation.
 *
 * Only 1 operation: get_page(hash).
 * Hash must be chosen carefully otherwise the page cache might return the same cached page for two different files / offsets.
 * Currently the IO engine appends the file descriptor number and the page offset to create a hash (fd << 40 + page_num).
 *
 * A hash is used to remember what is in the cache.
 * hash_to_page[hash].data1 = address of the page
 * hash_to_page[hash].data2 = lru entry of the page
 *
 * The lru entry is used to have a lru order of cached content + some metadata.
 * lru_entry.dirty = the page has been written but not flushed
 * lru_entry.contains_data = the page already contains the correct content, no need to read page from disk
 * These metadata are cleared by the page cache and set by the IO engine.
 *
 * The page cache shouldn't be used directly, the interface of the IO engine is a more convenient way to access data.
 */

void page_cache_init(struct pagecache *p) {
   declare_timer;
   start_timer {
      printf("#Reserving memory for page cache...\n");
      p->cached_data = aligned_alloc(PAGE_SIZE, PAGE_CACHE_SIZE/get_nb_workers());
      assert(p->cached_data); // If it fails here, it's probably because page cache size is bigger than RAM -- see options.h
      memset(p->cached_data, 0, PAGE_CACHE_SIZE/get_nb_workers());
   } stop_timer("Page cache initialization");

   p->hash_to_page = tree_create();
   p->used_pages = calloc(MAX_PAGE_CACHE/get_nb_workers(), sizeof(*p->used_pages));
   p->used_page_size = 0;
   p->oldest_page = NULL;
   p->newest_page = NULL;
}

struct lru *add_page_in_lru(struct pagecache *p, void *page, uint64_t hash) {
   struct lru *me = &p->used_pages[p->used_page_size];
   me->hash = hash;
   me->page = page;
   if(!p->oldest_page)
      p->oldest_page = me;
   me->prev = NULL;
   me->next = p->newest_page;
   if(p->newest_page)
      p->newest_page->prev = me;
   p->newest_page = me;
   return me;
}

void bump_page_in_lru(struct pagecache *p, struct lru *me, uint64_t hash) {
   assert(me->hash == hash);
   if(me == p->newest_page)
      return;
   if(p->oldest_page == me && me->prev)
      p->oldest_page = me->prev;
   if(me->prev)
      me->prev->next = me->next;
   if(me->next)
      me->next->prev = me->prev;
   me->prev = NULL;
   me->next = p->newest_page;
   p->newest_page->prev = me;
   p->newest_page = me;
}

/*
 * Get a page from the page cache.
 * *page will be set to the address in the page cache
 * @return 1 if the page already contains the right data, 0 otherwise.
 */
int get_page(struct pagecache *p, uint64_t hash, void **page, struct lru **lru) {
   void *dst;
   struct lru *lru_entry;
   maybe_unused pagecache_entry_t tmp_entry;
   maybe_unused pagecache_entry_t *old_entry = NULL;

   // Is the page already cached?
   pagecache_entry_t *e = tree_lookup(p->hash_to_page, hash);
   if(e) {
      dst = e->page;
      lru_entry = e->lru;
      if(lru_entry->hash != hash)
         die("LRU wierdness %lu vs %lu\n", lru_entry->hash, hash);
      bump_page_in_lru(p, lru_entry, hash);
      *page = dst;
      *lru = lru_entry;
      return 1;
   }


   // Otherwise allocate a new page, either a free one, or reuse the oldest
   if(p->used_page_size < MAX_PAGE_CACHE/get_nb_workers()) {
      dst = &p->cached_data[PAGE_SIZE*p->used_page_size];
      lru_entry = add_page_in_lru(p, dst, hash);
      p->used_page_size++;
   } else {
      lru_entry = p->oldest_page;
      dst = p->oldest_page->page;

      tree_delete(p->hash_to_page, p->oldest_page->hash, &old_entry);

      lru_entry->hash = hash;
      lru_entry->page = dst;
      bump_page_in_lru(p, lru_entry, hash);
   }

   // Remember that the page cache now stores this hash
   tree_insert(p->hash_to_page, hash, old_entry, dst, lru_entry);

   lru_entry->contains_data = 0;
   lru_entry->dirty = 0; // should already be equal to 0, but we never know
   *page = dst;
   *lru = lru_entry;

   return 0;
}
