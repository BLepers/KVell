#ifndef FREELIST_H
#define FREELIST_H

struct freelist_entry;
void add_item_in_free_list(struct slab *s, size_t idx, struct item_metadata *item);
void add_son_in_freelist(struct slab *s, size_t idx, struct item_metadata *item);
void get_free_item_idx(struct slab_callback *cb);

void add_item_in_free_list_recovery(struct slab *s, size_t idx, struct item_metadata *item);
void rebuild_free_list(struct slab *s);
void print_free_list(struct slab *s, int depth, struct item_metadata *item);
#endif
