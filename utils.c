#include "headers.h"
#include "utils.h"

static uint64_t freq = 0;
static uint64_t get_cpu_freq(void) {
   if(freq)
      return freq;

   FILE *fd;
   float freqf = 0;
   char *line = NULL;
   size_t len = 0;

   fd = fopen("/proc/cpuinfo", "r");
   if (!fd) {
      fprintf(stderr, "failed to get cpu frequency\n");
      perror(NULL);
      return freq;
   }

   while (getline(&line, &len, fd) != EOF) {
      if (sscanf(line, "cpu MHz\t: %f", &freqf) == 1) {
         freqf = freqf * 1000000UL;
         freq = (uint64_t) freqf;
         break;
      }
   }

   fclose(fd);
   return freq;
}


uint64_t cycles_to_us(uint64_t cycles) {
   return cycles*1000000LU/get_cpu_freq();
}

void shuffle(size_t *array, size_t n) {
   if (n > 1) {
      size_t i;
      for (i = 0; i < n - 1; i++) {
         size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
         size_t t = array[j];
         array[j] = array[i];
         array[i] = t;
      }
   }
}

void pin_me_on(int core) {
   if(!PINNING)
      return;

   cpu_set_t cpuset;
   pthread_t thread = pthread_self();

   CPU_ZERO(&cpuset);
   CPU_SET(core, &cpuset);

   int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0)
      die("Cannot pin thread on core %d\n", core);

}
