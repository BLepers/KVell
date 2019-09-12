#ifndef RANDOM_H
#define RANDOM_H 1

typedef long (*random_gen_t)(void);

unsigned long xorshf96(void);
unsigned long locxorshf96(void);

void init_seed(void); // must be called after each thread creation
void init_zipf_generator(long min, long max);
long zipf_next(); // zipf distribution, call init_zipf_generator first
long uniform_next(); // uniform, call init_zipf_generator first
long bogus_rand(); // returns something between 1 and 1000
long production_random1(void); // production workload simulator
long production_random2(void); // production workload simulator

const char *get_function_name(random_gen_t f);
#endif
