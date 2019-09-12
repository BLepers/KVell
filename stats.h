#ifndef STATS_H
#define STATS_H 1

struct slab_callback;
void add_timing_stat(uint64_t elapsed);
void print_stats(void);

uint64_t cycles_to_us(uint64_t cycles);

void *allocate_payload(void);
void free_payload(struct slab_callback *c);
void add_time_in_payload(struct slab_callback *c, size_t origin);
uint64_t get_time_from_payload(struct slab_callback *c, size_t pos);
uint64_t get_origin_from_payload(struct slab_callback *c, size_t pos);
#endif
