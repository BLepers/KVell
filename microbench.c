#include "headers.h"
#include "random.h"
#include "indexes/rbtree.h"
#include "indexes/rax.h"
#include "indexes/art.h"
#include "indexes/btree.h"
#include <sys/resource.h>
#include <errno.h>


/*
 * IO benchmarks
 */
#define NB_THREADS 6
#define NB_ACCESSES 5000000LU

#define RO 1 // read only
#define WO 2 // write only
#define RW 3 // read write
#define RM 4 // read mostly
struct pdata {
   int fd;
   int queue_size;
   size_t nb_accesses;
   size_t nb_pages;
   size_t rw;
};

static char *path = NULL;

static int io_setup(unsigned nr, aio_context_t *ctxp) {
	return syscall(__NR_io_setup, nr, ctxp);
}

static int io_destroy(aio_context_t ctx) {
	return syscall(__NR_io_destroy, ctx);
}

static int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp) {
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout) {
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

int get_worker_id() {
   return 0;
}

void *do_libaio(void *data) {
   struct pdata *pdata = data;
   int fd = pdata->fd;
   int queue_size = pdata->queue_size;
   size_t nb_accesses = pdata->nb_accesses;
   size_t nb_pages = pdata->nb_pages;
   unsigned int seed = rand();

   aio_context_t ctx;
   memset(&ctx, 0, sizeof(ctx));

   struct iocb cb[1024];
   struct iocb *cbs[1024];
   struct io_event events[1024];
   int ret;
   char *buffers = aligned_alloc(PAGE_SIZE, PAGE_SIZE * queue_size);

   ret = io_setup(1024, &ctx);
   if (ret < 0) {
      perror("io_setup");
      exit(-1);
   }

   declare_breakdown;

   for(size_t i = 0; i < nb_accesses; ) {
      memset(&cb, 0, sizeof(cb));


      for(size_t j = 0; j < queue_size; j++) {
         uint64_t page = rand_r(&seed) % nb_pages;

         cb[j].aio_fildes = fd;
         if(pdata->rw == RO) {
            cb[j].aio_lio_opcode = IOCB_CMD_PREAD;
         } else if(pdata->rw == WO) {
            cb[j].aio_lio_opcode = IOCB_CMD_PWRITE;
         } else if(pdata->rw == RW) {
            cb[j].aio_lio_opcode = (rand_r(&seed)%2)?IOCB_CMD_PWRITE:IOCB_CMD_PREAD;
         } else {
            cb[j].aio_lio_opcode = (rand_r(&seed)%100<5)?IOCB_CMD_PWRITE:IOCB_CMD_PREAD;
         }
         cb[j].aio_buf = (uint64_t)&buffers[PAGE_SIZE*j];
         cb[j].aio_offset = page * PAGE_SIZE;
         cb[j].aio_nbytes = PAGE_SIZE;

         cbs[j] = &cb[j];

         i++;
      }

      ret = io_submit(ctx, queue_size, cbs);

      if (ret != queue_size) {
         if (ret < 0) perror("io_submit");
         else fprintf(stderr, "io_submit only submitted %d\n", ret);
      } __1
      ret = io_getevents(ctx, ret, ret, events, NULL); __2

      //wait_for(450000); __3

      show_breakdown_periodic(1000, i, "io_submit", "io_getevents", "waiting", "unused", "unused", "unused", "");
   }

   ret = io_destroy(ctx);
   if (ret < 0) {
      perror("io_destroy");
      exit(-1);
   }

   free(pdata);
   free(buffers);

   return NULL;
}

const char* rw_to_str(size_t rw) {
   if(rw == RO)
      return "Read only";
   if(rw == WO)
      return "Write only";
   if(rw == RW)
      return "Read-write";
   if(rw == RM)
      return "Read-mostly";
   return "????";
}

int bench_io(void) {
   declare_timer;

   int fd;
   struct stat sb;
   size_t nb_pages;


   fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
   if(fd == -1)
      perr("Cannot open %s\n", path);

   fstat(fd, &sb);
   nb_pages = sb.st_size / PAGE_SIZE;
   printf("# Size of file being benched: %luB = %lu pages\n", sb.st_size, nb_pages);

   /* Direct IO perf */
   /*char page_data[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE)));
   start_timer {
      for(size_t i = 0; i < NB_ACCESSES; i++) {
         uint64_t page = xorshf96() % nb_pages;
         int ret = pread(fd, &page_data, PAGE_SIZE, page * PAGE_SIZE);
         assert(ret == PAGE_SIZE);
      }
   } stop_timer("DirectIO - Time for %lu accesses = %lums (%lu io/s)", NB_ACCESSES, elapsed/1000, NB_ACCESSES*1000000LU/elapsed);*/

   close(fd);
   fd = open(path,  O_RDWR | O_CREAT | O_NONBLOCK | O_DIRECT, 0777);

   /* libaio perf - various queue size */
   size_t queue_sizes[] = { 64 };
   for(size_t rw = WO; rw <= WO; rw++) {
      for(size_t q = 0; q < sizeof(queue_sizes)/sizeof(*queue_sizes); q++) {
         size_t queue_size = queue_sizes[q];

         start_timer {
            pthread_t threads[NB_THREADS];
            for(size_t i = 0; i < NB_THREADS; i++) {
               struct pdata *data = malloc(sizeof(*data));
               data->fd = fd;
               data->rw = rw;
               data->queue_size = queue_size;
               data->nb_accesses = NB_ACCESSES / NB_THREADS;
               data->nb_pages = nb_pages;
               pthread_create(&threads[i], NULL, do_libaio, data);
            }
            for(size_t i = 0; i < NB_THREADS; i++) {
               pthread_join(threads[i], NULL);
            }
         } stop_timer("libaio %d threads - %s - Time for %lu accesses queue size %lu = %lums (%lu io/s)", NB_THREADS, rw_to_str(rw), NB_ACCESSES, queue_size, elapsed/1000, NB_ACCESSES*1000000LU/elapsed);
      }
   }

   /* MMap perf */
   /*start_timer {
      char *map = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      for(size_t i = 0; i < NB_ACCESSES; i++) {
         uint64_t page = xorshf96() % nb_pages;
         memcpy(page_data, &map[page * PAGE_SIZE], PAGE_SIZE);
      }
   } stop_timer("MMAP - Time for %lu accesses = %lums (%lu io/s)", NB_ACCESSES, elapsed/1000, NB_ACCESSES*1000000LU/elapsed);*/

   return 0;
}


/*
 * Data structures tests
 */
#define NB_INSERTS 100000000LU

int bench_data_structures(void) {
   declare_timer;
   declare_memory_counter;


   /*
    * BTREE
    */
   btree_t * b = btree_create();

   start_timer {
      struct index_entry e;
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         btree_insert(b, (unsigned char*)&hash, sizeof(hash), &e);
      }
   } stop_timer("BTREE - Time for %lu inserts/replace (%lu inserts/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);

   start_timer {
      struct index_entry *e;
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         btree_find(b, (unsigned char*)&hash, sizeof(hash), &e);
      }
   } stop_timer("BTREE - Time for %lu finds (%lu finds/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);

   get_memory_usage("BTREE");


   /*
    * UTHASH - Not used because of latency spikes when resizing...
    */
   /*start_timer {
      struct hash *h = create_hash();
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         struct hash_entry e = {
            .hash = hash,
            .data1 = NULL,
            .data2 = NULL,
         };
         if(!find_entry(h, hash))
            add_entry(h, &e);
      }
   } stop_timer("HASH - Time for %lu inserts/replace (%lu inserts/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);*/

   return 0;
}

/*
 * Understand Zipf
 */
#define MAX_R 100000000LU
#define BENCH_L 100000000LU
struct counter {
   size_t i;
   size_t j;
};

int cmpfunc (const void * _a, const void * _b) {
   const struct counter *a = _a;
   const struct counter *b = _b;
   if(a->i > b->j)
      return -1;
   if(a->i < b->j)
      return 1;
   return 0;
}

void bench_zipf(void) {
   init_zipf_generator(0, MAX_R);
   struct counter *count = calloc(MAX_R, sizeof(*count));
   for(size_t i = 0; i < MAX_R; i++)
      count[i].i = i;
   for(size_t i = 0; i < BENCH_L; i++)
      count[zipf_next()].j++;
   qsort(count, MAX_R, sizeof(*count), cmpfunc);
   for(size_t i = 0; i < 100; i++)
      printf("%lu - %lu\n", count[i].i, count[i].j);
}

int main(int argc, char **argv) {
   path = "/scratch0/blepers/slab-0-0-0-1024";
   srand(time(NULL));
   bench_io();
   //bench_data_structures();
   //bench_zipf();
   return 0;
}

