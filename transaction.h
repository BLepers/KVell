#ifndef TRANSACTION_H
#define TRANSACTION_H

struct transaction;

/* OLTP / OLAP */
struct transaction *create_transaction(void);
void kv_trans_read(struct transaction *t, struct slab_callback *callback);
void kv_trans_write(struct transaction *t, struct slab_callback *callback);
int kv_commit(struct transaction *t, struct slab_callback *callback);
int kv_abort(struct transaction *t, struct slab_callback *callback); // either commit or abort *have* to be called, even if transaction fails (to free memory)

/* OLCP */
void kv_long_scan(struct slab_callback *cb);


/* Private */
struct transaction *create_long_transaction(struct slab_callback *map, void *payload);
void transaction_propagate(void *item, uint64_t max_snapshot_id);
struct slab_callback *get_map(struct transaction *t);

size_t get_transaction_id(struct transaction *t);
size_t get_transaction_id_on_disk(struct transaction *t);
uint64_t get_snapshot_version(struct transaction *t);

int has_failed(struct transaction *t);

void set_payload(struct transaction *t, void *data);
void *get_payload(struct transaction *t);

uint64_t get_start_time(struct transaction *t);
int is_complete(struct transaction *t);
int is_in_commit(struct transaction *t);

void set_transaction_id(struct transaction *t, uint64_t v);
void set_snapshot_version(struct transaction *t, uint64_t v);

struct transaction *transaction_get_long_next(struct transaction *t);
struct transaction *transaction_get_long_prev(struct transaction *t);
void transaction_set_long_next(struct transaction *t, struct transaction *next);
void transaction_set_long_prev(struct transaction *t, struct transaction *prev);

void register_next_batch(struct slab_callback *cb, size_t size, size_t *hashes);

int action_allowed(index_entry_t *e, struct slab_callback *cb);
#endif
