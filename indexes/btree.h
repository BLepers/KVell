#ifndef BTREE_H
#define BTREE_H

#ifdef __cplusplus
extern "C" {
#endif

   /* Items stored in the btree have a "transaction_id" flag that contains the id of the transaction that is writting the item.
    * We also add a flag in this field. If transaction_id & ITEM_CONTAINS_NEW_IDX, then the index points to the new value (after commit), otherwise the old value. */

#include "memory-item.h"
typedef void btree_t;

btree_t *btree_create();
size_t btree_size(btree_t *t);
int btree_find(btree_t *t, unsigned char*k, size_t len, struct index_entry **e);
int btree_find_next(btree_t *t, unsigned char* k, size_t len, struct index_entry **e, uint64_t *found_hash);
void btree_delete(btree_t *t, unsigned char*k, size_t len);
void btree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e);

void btree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data);
void btree_free(btree_t *t);

#ifdef __cplusplus
}
#endif

#endif
