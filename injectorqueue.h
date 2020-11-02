#ifndef INJECTOR_QUEUE_H
#define INJECTOR_QUEUE_H

struct injector_queue;

struct injector_queue *create_new_injector_queue(void);
void injector_insert_in_queue(int worker_id, struct injector_queue *q, struct slab_callback *cb, void *item);
int injector_process_queue(struct injector_queue *q);
int injector_has_pending_work(struct injector_queue *q);

void injector_print_queue_of(struct injector_queue *q, uint64_t transaction_id); // debug
#endif
