#include "headers.h"

int get_nb_workers(void) {
   return 1;
}

#define NB_PAGECACHE_ACCESSES 10000000LU
static struct pagecache *p;
void bench_pagecache(void) {
   declare_timer;
   p = malloc(sizeof(*p));
   page_cache_init(p);

   start_timer {
      void *page;
      struct lru *lru;
      for(size_t i = 0; i < PAGE_CACHE_SIZE/PAGE_SIZE; i++) {
         uint64_t hash = i;
         get_page(p, hash, &page, &lru);
      }
   } stop_timer("Filling the page cache: %lu ops, %lu ops/s\n", PAGE_CACHE_SIZE/PAGE_SIZE, PAGE_CACHE_SIZE/PAGE_SIZE*1000000LU/elapsed);

   start_timer {
      void *page;
      struct lru *lru;
      for(size_t i = 0; i < NB_PAGECACHE_ACCESSES; i++) {
         uint64_t hash = xorshf96() % (PAGE_CACHE_SIZE/PAGE_SIZE);
         get_page(p, hash, &page, &lru);
      }
   } stop_timer("Accessing existing pages %lu ops, %lu ops/s\n", NB_PAGECACHE_ACCESSES, NB_PAGECACHE_ACCESSES*1000000LU/elapsed);

   start_timer {
      void *page;
      struct lru *lru;
      for(size_t i = 0; i < NB_PAGECACHE_ACCESSES; i++) {
         uint64_t hash = xorshf96() + PAGE_CACHE_SIZE/PAGE_SIZE;
         get_page(p, hash, &page, &lru);
      }
   } stop_timer("Accessing non cached pages %lu ops, %lu ops/s\n", NB_PAGECACHE_ACCESSES, NB_PAGECACHE_ACCESSES*1000000LU/elapsed);
}

int main(int argc, char **argv) {
   bench_pagecache();
   return 0;
}

