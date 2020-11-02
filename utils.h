#ifndef UTILS_H
#define UTILS_H 1
#include "headers.h"
#include "stats.h"

#define PAGE_SIZE (4LU*1024LU)
#define ONE_GB (1024*1024*1024)

#define die(msg, args...) \
   do {                         \
      fprintf(stderr,"(%s,%d) " msg "\n", __FUNCTION__ , __LINE__, ##args); \
      fflush(stdout); \
      exit(-1); \
   } while(0)

#define perr(msg, args...) \
   do {                        \
      perror("Error: "); \
      fprintf(stderr,"(%s,%d) " msg "\n", __FUNCTION__ , __LINE__, ##args); \
      exit(-1);   \
   } while(0)


#ifdef __x86_64__
#define rdtscll(val) {                                           \
       unsigned int __a,__d;                                        \
       asm volatile("rdtsc" : "=a" (__a), "=d" (__d));              \
       (val) = ((unsigned long)__a) | (((unsigned long)__d)<<32);   \
}
#else
#define rdtscll(val) __asm__ __volatile__("rdtsc" : "=A" (val))
#endif

#define NOP10() asm("nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;")

#define maybe_unused __attribute__((unused))

#define STRINGIZE_(x) #x
#define STRINGIZE(x) STRINGIZE_(x)

/*
 * Cute timer macros
 * Usage:
 * declare_timer;
 * start_timer {
 *   ...
 * } stop_timer("Took %lu us", elapsed);
 */
#define declare_timer uint64_t elapsed; \
   struct timeval st, et;

#define start_timer gettimeofday(&st,NULL);

#define stop_timer(msg, args...) ;do { \
   gettimeofday(&et,NULL); \
   elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec) + 1; \
   printf("(%s,%d) [%6lums] " msg "\n", __FUNCTION__ , __LINE__, elapsed/1000, ##args); \
} while(0)

/*
 * Cute timer macros
 */
#define struct_declare_timer uint64_t elapsed; \
   struct timeval st, et;

#define struct_start_timer(s) gettimeofday(&(s)->st,NULL);

#define struct_stop_timer(s, msg, args...) ;do { \
   gettimeofday(&(s)->et,NULL); \
   (s)->elapsed = (((s)->et.tv_sec - (s)->st.tv_sec) * 1000000) + ((s)->et.tv_usec - (s)->st.tv_usec) + 1; \
   printf("(%s,%d) [%6lums] " msg "\n", __FUNCTION__ , __LINE__, (s)->elapsed/1000, ##args); \
} while(0)

/*
 * Cute debug timer.
 * declare_debug_timer;
 * start_debug_timer {
 *   ...
 * } stop_debug_timer(1000, "WARNING, this section took more than 1ms!");
 */
#define declare_debug_timer __attribute__((unused)) uint64_t __s, __e;

#if DEBUG

#define start_debug_timer rdtscll(__s);

#define stop_debug_timer(alert_thres, msg, args...) ; do { \
   rdtscll(__e); \
   uint64_t __elapsed = cycles_to_us(__e - __s); \
   if(__elapsed > (alert_thres)) \
      printf("(%s,%d) [%6luus] " msg "\n", __FUNCTION__ , __LINE__, __elapsed, ##args); \
} while(0)

#else

#define start_debug_timer
#define stop_debug_timer(alert_thres, msg, args...)

#endif

/*
 * Foreach macro
 */
#define foreach(item, array) \
    for(int keep = 1, \
            count = 0,\
            size = sizeof (array) / sizeof *(array); \
        keep && count != size; \
        keep = !keep, count++) \
      for(item = *((array) + count); keep; keep = !keep)

/*
 * Cute way to print a message periodically in a loop.
 * declare_periodic_count;
 * for(...) {
 *    periodic_count(1000, "Hello"); // prints hello and the number of iterations every second
 * }
 */
#define declare_periodic_count \
      uint64_t __real_start = 0, __start, __last, __nb_count; \
      if(!__real_start) { \
         rdtscll(__real_start); \
         __start = __real_start; \
         __nb_count = 0; \
      }

#define periodic_count(period, msg, args...) \
   do { \
      rdtscll(__last); \
      __nb_count++; \
      if(cycles_to_us(__last - __start) > ((period)*1000LU)) { \
         printf("(%s,%d) [%3lus] [%7lu ops/s] " msg "\n", __FUNCTION__ , __LINE__, cycles_to_us(__last - __real_start)/1000000LU, __nb_count*1000000LU/cycles_to_us(__last - __start), ##args); \
         __nb_count = 0; \
         __start = __last; \
      } \
   } while(0);


/*
 * Cute way to get a breakdown of where time is spent.
 * declare_breakdown;
 * for(...) {
 *    fun1(); __1
 *    fun2(); __2
 *    ...
 *    show_breakdown_periodic(1000, "fun1", "fun2", ...); // "fun1 X% fun2 Y%" every second
 * }
 */
#define declare_breakdown \
   struct __breakdown { \
      uint64_t real_start; \
      uint64_t start; \
      uint64_t now;  \
      uint64_t evt1; \
      uint64_t evt2; \
      uint64_t evt3; \
      uint64_t evt4; \
      uint64_t evt5; \
      uint64_t evt6; \
      uint64_t loops; \
      uint64_t count; \
   } __breakdown = {}; \
   if(!__breakdown.real_start) { \
      rdtscll(__breakdown.real_start); \
      __breakdown.start = __breakdown.real_start; \
   } else { \
      rdtscll(__breakdown.start); \
   }

#define __1 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt1 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define __2 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt2 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define __3 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt3 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define __4 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt4 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define __5 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt5 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define __6 \
   do { \
      rdtscll(__breakdown.now); \
      __breakdown.evt6 += __breakdown.now - __breakdown.start; \
      __breakdown.start = __breakdown.now; \
   } while(0);

#define show_breakdown_periodic(period, _count, _evt1, _evt2, _evt3, _evt4, _evt5, _evt6, msg, args...) \
   do { \
      __breakdown.loops++; \
      rdtscll(__breakdown.now); \
      uint64_t elapsed = __breakdown.now - __breakdown.real_start; \
      if(cycles_to_us(elapsed) > ((period)*1000LU)) { \
         uint64_t count_diff = _count - __breakdown.count; \
         __breakdown.count = _count; \
         printf("[WORKER %02d BREAKDOWN] " _evt1 "%3lu%% - " _evt2 " %3lu%% - " _evt3 " %3lu%% - " _evt4 " %3lu%% - " _evt5 " %3lu%% - " _evt6 " %3lu%% - %7lu ops - %7lu ops/s - %3lu ops / loop - %lu cycles/op" msg "\n", \
               get_worker_id(), \
               __breakdown.evt1*100LU/elapsed, \
               __breakdown.evt2*100LU/elapsed, \
               __breakdown.evt3*100LU/elapsed, \
               __breakdown.evt4*100LU/elapsed, \
               __breakdown.evt5*100LU/elapsed, \
               __breakdown.evt6*100LU/elapsed, \
               count_diff, \
               count_diff * period * 1000 / cycles_to_us(elapsed), \
               count_diff/__breakdown.loops, \
               count_diff?(elapsed / count_diff):0, \
               ##args); \
         __breakdown.real_start = __breakdown.now; \
         __breakdown.evt1 = 0; \
         __breakdown.evt2 = 0; \
         __breakdown.evt3 = 0; \
         __breakdown.evt4 = 0; \
         __breakdown.evt5 = 0; \
         __breakdown.evt6 = 0; \
         __breakdown.loops = 0; \
      } \
   } while(0);

#endif

/*
 * Cute way to measure memory usage.
 */
#define declare_memory_counter \
   struct rusage __memory; \
   uint64_t __previous_mem = 0, __used_mem; \
   getrusage(RUSAGE_SELF, &__memory); \
   __previous_mem = __memory.ru_maxrss;

#define get_memory_usage(msg, args...) \
   getrusage(RUSAGE_SELF, &__memory); \
   __used_mem = __memory.ru_maxrss - __previous_mem; \
   __previous_mem = __memory.ru_maxrss; \
   printf("(%s,%d) [%lu MB total - %lu MB since last measure] " msg "\n", __FUNCTION__ , __LINE__, __previous_mem/1024, __used_mem/1024, ##args);

/*
 * Per thread timing statistics macro
 */
#define declare_periodic_overhead \
      uint64_t __real_start = 0, __last_dump, __start, __last, __nb_count = 0, __total = 0, maybe_unused __last_count = 0; \
      if(!__real_start) { \
         rdtscll(__real_start); \
         __last_dump = __real_start; \
         __nb_count = 0; \
      } \

#define start_periodic_overhead rdtscll(__start);

#define stop_periodic_overhead(period, name, msg, args...) \
   do { \
      rdtscll(__last); \
      __total += __last - __start; \
      __nb_count ++; \
      if(cycles_to_us(__last - __last_dump) > ((period)*1000LU)) { \
         printf("[" name "] " msg " %lu%% - %7lu ops/s\n", ##args, __total*100LU/(__last - __last_dump), __nb_count*1000000LU/cycles_to_us(__last - __last_dump)); \
         __nb_count = 0; \
         __last_dump = __last; \
         __total = 0; \
      } \
   } while(0);

#define stop_periodic_overhead2(period, count, name, msg, args...) \
   do { \
      rdtscll(__last); \
      __total += __last - __start; \
      __nb_count += (count) - __last_count; \
      __last_count = (count); \
      if(cycles_to_us(__last - __last_dump) > ((period)*1000LU)) { \
         printf("[" name "] " msg " %lu%% - %7lu ops/s\n", ##args, __total*100LU/(__last - __last_dump), __nb_count*1000000LU/cycles_to_us(__last - __last_dump)); \
         __nb_count = 0; \
         __last_dump = __last; \
         __total = 0; \
      } \
   } while(0);

/*
 * Busy waiting
 */
#define wait_for(cycles) \
   do { \
      uint64_t _s, _e; \
      rdtscll(_s); \
      while(1) { \
         rdtscll(_e); \
         if(_e - _s >= cycles) \
            break; \
      } \
   } while(0);

/*
 * Helper functions
 */
uint64_t cycles_to_us(uint64_t cycles);
void shuffle(size_t *array, size_t n);
void pin_me_on(int core);
