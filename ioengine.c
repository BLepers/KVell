#include "headers.h"

/*
 * Asynchronous IO engine.
 *
 * Two operations: read a page and write a page.
 * Both operations work closely with the page cache.
 *
 * Reading a page will first look if the page is cached. If it is, then it calls the callback synchronously.
 * If the page is not cached, a page will be allocated in the page cache and the callback will be called asynchronously.
 * The data is in callback->lru_entry->page.
 * e.g. read_page_async(fd, page_num, my_callback)
 *      my_callback(cb) {
 *          cb->lru_entry // the page cache metadata of the page where the data has been loaded
 *          cb->lru_entry->page // the page that contains the data
 *      }
 *
 * Writing a page consists in flushing the content of the page cache to disk.
 * It means the page must be in memory, it is not possible to write a non cached page.
 * This could be easilly changed if need be.
 *
 * ASSUMPTIONS:
 *   The page cache is big enough to hold as many pages as concurrent buffered IOs.
 */

/*
 * Non asynchronous calls to ease some things
 */
static __thread char *disk_data;
void *safe_pread(int fd, off_t offset) {
   if(!disk_data)
      disk_data = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
   int r = pread(fd, disk_data, PAGE_SIZE, offset);
   if(r != PAGE_SIZE)
      perr("pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, offset);
   return disk_data;
}

/*
 * Async API definition
 */
static int io_setup(unsigned nr, aio_context_t *ctxp) {
	return syscall(__NR_io_setup, nr, ctxp);
}

static int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp) {
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout) {
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


/*
 * Definition of the context of an IO worker thread
 */
struct linked_callbacks {
   struct slab_callback *callback;
   struct linked_callbacks *next;
};
struct io_context {
   aio_context_t ctx __attribute__((aligned(64)));
   volatile size_t sent_io;
   volatile size_t processed_io;
   size_t max_pending_io;
   size_t ios_sent_to_disk;
   struct iocb *iocb;
   struct iocb **iocbs;
   struct io_event *events;
   struct linked_callbacks *linked_callbacks;
};

/*
 * After completing IOs we need to call all the "linked callbacks", i.e., reads done to a page that was already in the process of being fetched.
 */
static void process_linked_callbacks(struct io_context *ctx) {
   declare_debug_timer;

   size_t nb_linked = 0;
   start_debug_timer {
      struct linked_callbacks *linked_cb;
      linked_cb = ctx->linked_callbacks;
      ctx->linked_callbacks = NULL; // reset the list

      while(linked_cb) {
         struct linked_callbacks *next = linked_cb->next;
         struct slab_callback *callback = linked_cb->callback;
         if(callback->lru_entry->contains_data) {
            callback->io_cb(callback);
            free(linked_cb);
         } else { // page has not been prefetched yet, it's likely in the list of pages that will be read during the next kernel call
            linked_cb->next = ctx->linked_callbacks;
            ctx->linked_callbacks = linked_cb; // re-link our callback
         }
         linked_cb = next;
         nb_linked++;
      }
   } stop_debug_timer(10000, "%lu linked callbacks\n", nb_linked);
}

/*
 * Loop executed by worker threads
 */
static void worker_do_io(struct io_context *ctx) {
   size_t pending = ctx->sent_io - ctx->processed_io;
   if(pending == 0) {
      ctx->ios_sent_to_disk = 0;
      return;
   }
   /*if(pending > QUEUE_DEPTH)
      pending = QUEUE_DEPTH;*/

   for(size_t i = 0; i < pending; i++) {
      struct slab_callback *callback;
      ctx->iocbs[i] = &ctx->iocb[(ctx->processed_io + i)%ctx->max_pending_io];
      callback = (void*)ctx->iocbs[i]->aio_data;
      callback->lru_entry->dirty = 0;  // reset the dirty flag *before* sending write orders otherwise following writes might be ignored
                                       // race condition if flag is reset after:
                                       //        io_submit
                                       //        flush done to disk
                                       //              |                           write page (no IO order because dirty = 1, see write_page_async "if(lru_entry->dirty)" condition)
                                       //        complete ios
                                       //        (old value written to disk)

      add_time_in_payload(callback, 3);
   }

   // Submit requests to the kernel
   int ret = io_submit(ctx->ctx, pending, ctx->iocbs);
   if (ret != pending)
      perr("Couldn't submit all io requests! %d submitted / %lu (%lu sent, %lu processed)\n", ret, pending, ctx->sent_io, ctx->processed_io);
   ctx->ios_sent_to_disk = ret;
}


/*
 * do_io = wait for all requests to be completed
 */
void do_io(void) {
   // TODO
}

/* We need a unique hash for each page for the page cache */
static uint64_t get_hash_for_page(int fd, uint64_t page_num) {
   return (((uint64_t)fd)<<40LU)+page_num; // Works for files less than 40EB
}

/* Enqueue a request to read a page */
char *read_page_async(struct slab_callback *callback) {
   int alread_used;
   struct lru *lru_entry;
   void *disk_page;
   uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);
   struct io_context *ctx = get_io_context(callback->slab->ctx);
   uint64_t hash = get_hash_for_page(callback->slab->fd, page_num);

   alread_used = get_page(get_pagecache(callback->slab->ctx), hash, &disk_page, &lru_entry);
   callback->lru_entry = lru_entry;
   if(lru_entry->contains_data) {   // content is cached already
      callback->io_cb(callback);       // call the callback directly
      return disk_page;
   }

   if(alread_used) { // Somebody else is already prefetching the same page!
      struct linked_callbacks *linked_cb = malloc(sizeof(*linked_cb));
      linked_cb->callback = callback;
      linked_cb->next = ctx->linked_callbacks;
      ctx->linked_callbacks = linked_cb; // link our callback
      return NULL;
   }

   int buffer_idx = ctx->sent_io % ctx->max_pending_io;
   struct iocb *_iocb = &ctx->iocb[buffer_idx];
   memset(_iocb, 0, sizeof(*_iocb));
   _iocb->aio_fildes = callback->slab->fd;
   _iocb->aio_lio_opcode = IOCB_CMD_PREAD;
   _iocb->aio_buf = (uint64_t)disk_page;
   _iocb->aio_data = (uint64_t)callback;
   _iocb->aio_offset = page_num * PAGE_SIZE;
   _iocb->aio_nbytes = PAGE_SIZE;
   if(ctx->sent_io - ctx->processed_io >= ctx->max_pending_io)
      die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", ctx->sent_io, ctx->processed_io, ctx->max_pending_io);
   ctx->sent_io++;

   return NULL;
}

/* Enqueue a request to write a page, the lru entry must contain the content of the page (obviously) */
char *write_page_async(struct slab_callback *callback) {
   struct io_context *ctx = get_io_context(callback->slab->ctx);
   struct lru *lru_entry = callback->lru_entry;
   void *disk_page = lru_entry->page;
   uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);

   if(!lru_entry->contains_data) {  // page is not in RAM! Abort!
      die("WTF?\n");
   }

   if(lru_entry->dirty) { // this is the second time we write the page, which means it already has been queued for writting
      struct linked_callbacks *linked_cb;
      linked_cb = malloc(sizeof(*linked_cb));
      linked_cb->callback = callback;
      linked_cb->next = ctx->linked_callbacks;
      ctx->linked_callbacks = linked_cb; // link our callback
      return disk_page;
   }

   lru_entry->dirty = 1;

   int buffer_idx = ctx->sent_io % ctx->max_pending_io;
   struct iocb *_iocb = &ctx->iocb[buffer_idx];
   memset(_iocb, 0, sizeof(*_iocb));
   _iocb->aio_fildes = callback->slab->fd;
   _iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
   _iocb->aio_buf = (uint64_t)disk_page;
   _iocb->aio_data = (uint64_t)callback;
   _iocb->aio_offset = page_num * PAGE_SIZE;
   _iocb->aio_nbytes = PAGE_SIZE;
   if(ctx->sent_io - ctx->processed_io >= ctx->max_pending_io)
      die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", ctx->sent_io, ctx->processed_io, ctx->max_pending_io);
   ctx->sent_io++;

   return NULL;
}

/*
 * Init an IO worker
 */
struct io_context *worker_ioengine_init(size_t nb_callbacks) {
   int ret;
   struct io_context *ctx = calloc(1, sizeof(*ctx));
   ctx->max_pending_io = nb_callbacks * 2;
   ctx->iocb = calloc(ctx->max_pending_io, sizeof(*ctx->iocb));
   ctx->iocbs = calloc(ctx->max_pending_io, sizeof(*ctx->iocbs));
   ctx->events = calloc(ctx->max_pending_io, sizeof(*ctx->events));

   ret = io_setup(ctx->max_pending_io, &ctx->ctx);
   if(ret < 0)
      perr("Cannot create aio setup\n");

   return ctx;
}

/* Enqueue requests */
void worker_ioengine_enqueue_ios(struct io_context *ctx) {
   worker_do_io(ctx); // Process IO queue
}

/* Get processed requests from disk and call callbacks */
void worker_ioengine_get_completed_ios(struct io_context *ctx) {
   int ret = 0;
   declare_debug_timer;

   if(ctx->ios_sent_to_disk == 0)
      return;

   start_debug_timer {
      ret = io_getevents(ctx->ctx, ctx->ios_sent_to_disk - ret, ctx->ios_sent_to_disk - ret, &ctx->events[ret], NULL);
      if(ret != ctx->ios_sent_to_disk)
         die("Problem: only got %d answers out of %lu enqueued IO requests\n", ret, ctx->ios_sent_to_disk);
   } stop_debug_timer(10000, "io_getevents took more than 10ms!!");
}


void worker_ioengine_process_completed_ios(struct io_context *ctx) {
   int ret = ctx->ios_sent_to_disk;
   declare_debug_timer;

   if(ctx->ios_sent_to_disk == 0)
      return;


   start_debug_timer {
      // Enqueue completed IO requests
      for(size_t i = 0; i < ret; i++) {
         struct iocb *cb = (void*)ctx->events[i].obj;
         struct slab_callback *callback = (void*)cb->aio_data;
         assert(ctx->events[i].res == 4096); // otherwise page hasn't been read
         callback->lru_entry->contains_data = 1;
         //callback->lru_entry->dirty = 0; // done before
         callback->io_cb(callback);
      }

      // We might have "linked callbacks" so process them
      process_linked_callbacks(ctx);
   } stop_debug_timer(10000, "rest of worker_ioengine_process_completed_ios (%d requests)", ret);

   // Ok, now the main thread can push more requests
   ctx->processed_io += ctx->ios_sent_to_disk;
}

int io_pending(struct io_context *ctx) {
   return ctx->sent_io - ctx->processed_io;
}
