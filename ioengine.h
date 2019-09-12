#ifndef IOENGINE_H
#define IOENGINE_H 1


struct io_context *worker_ioengine_init(size_t nb_callbacks);

void *safe_pread(int fd, off_t offset);

typedef void (io_cb_t)(struct slab_callback *);
char *read_page_async(struct slab_callback *cb);
char *write_page_async(struct slab_callback *cb);

int io_pending(struct io_context *ctx);

void worker_ioengine_enqueue_ios(struct io_context *ctx);
void worker_ioengine_get_completed_ios(struct io_context *ctx);
void worker_ioengine_process_completed_ios(struct io_context *ctx);



#endif
