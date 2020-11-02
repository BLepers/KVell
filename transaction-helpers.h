#ifndef TRANSACTION_HELPERS
#define TRANSACTION_HELPERS 1

void init_transaction_manager(void);

uint64_t register_new_transaction(struct transaction *t); // create a transaction
size_t register_commit_transaction(size_t id);            // indicate that a transaction is committing - returns the ID on disk
void register_start_commit(struct transaction *t); // commit or abort is starting
void register_end_transaction(size_t id, size_t id_on_disk); // commit or abort is complete

size_t get_min_transaction_id(void);
size_t get_min_in_commit(void);
uint64_t get_min_snapshot_id(void);

size_t get_nb_running_transactions(void);
uint64_t get_max_recorded_parallel_transactions(void);
void forall_long_running_transaction(size_t min_rdt, size_t max_rdt, void *data);
#endif
