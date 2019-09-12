#ifndef BTREE_H
#define BTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "memory-item.h"
typedef void btree_t;

btree_t *btree_create();
int btree_find(btree_t *t, unsigned char*k, size_t len, struct index_entry *e);
void btree_delete(btree_t *t, unsigned char*k, size_t len);
void btree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e);
struct index_scan btree_find_n(btree_t *t, unsigned char* k, size_t len, size_t n);

void btree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data);
void btree_free(btree_t *t);

#ifdef __cplusplus
}
#endif

#endif
