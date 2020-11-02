#ifndef FREELIST_H
#define FREELIST_H

/* Adding a free spot */
void add_item_in_partially_freed_list(struct slab *s, size_t idx, size_t next_rdt);

/* Reusing a free spot */
void get_free_item_idx(struct slab_callback *cb);

#endif
