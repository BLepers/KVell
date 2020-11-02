#ifndef SLAB_WORKER_H
#define SLAB_WORKER_H 1

#include "pagecache.h"

struct slab_callback;
struct slab_context;
typedef struct index_scan tree_scan_res_t;

/*
 * Interface for KVell
 */
void kv_read_async(struct slab_callback *callback);
void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx);
void kv_read_for_write_async(struct slab_callback *callback);
void kv_add_async(struct slab_callback *callback);
void kv_update_async(struct slab_callback *callback);
void kv_update_in_place_async(struct slab_callback *callback);
void kv_lock_async(struct slab_callback *callback);
void kv_add_or_update_async(struct slab_callback *callback);
void kv_remove_async(struct slab_callback *callback);


size_t get_database_size(void);

/*
 * Private definitions
 */
void kv_start_commit(struct slab_callback *callback);
void kv_end_commit(struct slab_callback *callback);
void kv_revert_update(struct slab_callback *callback);
void kv_read_next_async(struct slab_callback *callback, int worker);
void kv_read_next_batch_async(struct slab_callback *callback, int worker);
void *kv_read_sync_safe(void *item);

void slab_workers_init(int nb_disks, int nb_workers_per_disk);
int get_nb_workers(void);

/*
 * Getters/setters for the slab context of a worker
 */
struct pagecache *get_pagecache(struct slab_context *ctx);
struct io_context *get_io_context(struct slab_context *ctx);
uint64_t get_rdt(struct slab_context *ctx);
void set_rdt(struct slab_context *ctx, uint64_t val);
struct to_be_freed_list *get_gc_for_item(char *item);
struct slab *get_item_slab(void *item);


/*
 * Configuration of the DB
 */
uint64_t get_highest_rdt(void);
int get_nb_disks(void);
size_t pending_work(void);


int get_worker(struct slab *s);
int get_worker_for_item(void *item);
int get_worker_for_prefix(uint64_t prefix);

void call_callback(struct slab_callback *callback, void *item);
void check_races_and_call(struct slab_callback *callback, void *item);
int is_worker_context(void);
int get_worker_id(void);

#endif
