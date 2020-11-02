#ifndef GC_H
#define GC_H

struct to_be_freed_list;

struct to_be_freed_list *init_gc(void);
void do_deletions(uint64_t worker_id, struct to_be_freed_list *l);
void add_item_in_gc(struct to_be_freed_list *l, struct slab_callback *cb, uint64_t index_rdt);

size_t gc_size(struct to_be_freed_list *l);

#endif

