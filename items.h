#ifndef ITEMS_H
#define ITEMS_H

#include "headers.h"

/*
 * Items are the data that is persisted on disk.
 * The metadata structs should not contain any pointer because they are persisted on disk.
 */

struct item_metadata {
   size_t rdt;
   size_t key_size;
   size_t value_size;
   // key
   // value
};

#endif
