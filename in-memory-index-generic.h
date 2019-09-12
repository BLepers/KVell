#ifndef IN_MEMORY_INDEX_GENERIC
#define IN_MEMORY_INDEX_GENERIC 1

#if MEMORY_INDEX == RBTREE
#include "in-memory-index-rbtree.h"
#elif MEMORY_INDEX == RAX
#include "in-memory-index-rax.h"
#elif MEMORY_INDEX == ART
#include "in-memory-index-art.h"
#elif MEMORY_INDEX == BTREE
#include "in-memory-index-btree.h"
#endif

#endif
