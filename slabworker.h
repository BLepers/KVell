#ifndef SLAB_WORKER_H
#define SLAB_WORKER_H 1

#include "pagecache.h"

struct slab_callback;
struct slab_context;

void kv_read_async(struct slab_callback *callback);
void kv_add_async(struct slab_callback *callback);
void kv_update_async(struct slab_callback *callback);
void kv_add_or_update_async(struct slab_callback *callback);
void kv_remove_async(struct slab_callback *callback);

typedef struct index_scan tree_scan_res_t;
tree_scan_res_t kv_init_scan(void *item, size_t scan_size);
void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx);

size_t get_database_size(void);


void slab_workers_init(int nb_disks, int nb_workers_per_disk);
int get_nb_workers(void);
void *kv_read_sync(void *item); // Unsafe
struct pagecache *get_pagecache(struct slab_context *ctx);
struct io_context *get_io_context(struct slab_context *ctx);
uint64_t get_rdt(struct slab_context *ctx);
void set_rdt(struct slab_context *ctx, uint64_t val);
int get_worker(struct slab *s);
int get_nb_disks(void);
struct slab *get_item_slab(int worker_id, void *item);
size_t get_item_size(char *item);
#endif
