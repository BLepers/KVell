#include "headers.h"
#include "utils.h"
#include "items.h"
#include "slab.h"
#include "ioengine.h"
#include "pagecache.h"
#include "slabworker.h"

/*
 * A slab is a file containing 1 or more items of a given size.
 * The size of items is in slab->item_size.
 *
 * Format is [ [size_t rdt1, size_t key_size1, size_t value_size1][key1][value1][maybe some empty space]     [rdt2, key_size2, value_size2][key2]etc. ]
 *
 * When an idem is deleted its key_size becomes -1. value_size is then equal to a next free idx in the slab.
 * That way, when we reuse an empty spot, we know where the next one is.
 *
 *
 * This whole file assumes that when a file is newly created, then all the data is equal to 0. This should be true on Linux.
 */

/*
 * Where is my item in the slab?
 */
off_t item_page_num(struct slab *s, size_t idx) {
   size_t items_per_page = PAGE_SIZE/s->item_size;
   return idx / items_per_page;
}
static off_t item_in_page_offset(struct slab *s, size_t idx) {
   size_t items_per_page = PAGE_SIZE/s->item_size;
   return (idx % items_per_page)*s->item_size;
}

/*
 * When first loading a slab from disk we need to rebuild the in memory tree, these functions do that.
 */
void add_existing_item(struct slab *s, size_t idx, void *_item, struct slab_callback *callback) {
   struct item_metadata *item = _item;
   if(item->key_size == -1) { // Removed item
      add_item_in_free_list_recovery(s, idx, item);
      if(idx > s->last_item)
         s->last_item = idx;
   } else if(item->key_size != 0) {
      s->nb_items++;
      if(idx > s->last_item)
         s->last_item = idx;
      if(item->rdt > get_rdt(s->ctx)) // Remember the maximum timestamp existing in the DB
         set_rdt(s->ctx, item->rdt);
      if(callback) { // Call the user callback if it exists
         callback->slab_idx = idx;
         callback->cb(callback, item);
      }
   } else {
      //printf("Empty item on page #%lu idx %lu\n", page_num, idx);
   }
}

void process_existing_chunk(int slab_worker_id, struct slab *s, size_t nb_files, size_t file_idx, char *data, size_t start, size_t length, struct slab_callback *callback) {
   static __thread declare_periodic_count;
   size_t nb_items_per_page = PAGE_SIZE / s->item_size;
   size_t nb_pages = length / PAGE_SIZE;
   for(size_t p = 0; p < nb_pages; p++) {
      size_t page_num = ((start + p*PAGE_SIZE) / PAGE_SIZE); // Physical page to virtual page
      size_t base_idx = page_num*nb_items_per_page*nb_files + file_idx*nb_items_per_page;
      size_t current = p*PAGE_SIZE;
      for(size_t i = 0; i < nb_items_per_page; i++) {
         add_existing_item(s, base_idx, &data[current], callback);
         base_idx++;
         current += s->item_size;
         periodic_count(1000, "[SLAB WORKER %d] Init - Recovered %lu items, %lu free spots", slab_worker_id, s->nb_items, s->nb_free_items);
      }
   }
}

#define GRANULARITY_REBUILD (2*1024*1024) // We rebuild 2MB by 2MB
void rebuild_index(int slab_worker_id, struct slab *s, struct slab_callback *callback) {
   char *cached_data = aligned_alloc(PAGE_SIZE, GRANULARITY_REBUILD);

   int fd = s->fd;
   size_t start = 0, end;
   while(1) {
      end = start + GRANULARITY_REBUILD;
      if(end > s->size_on_disk)
         end = s->size_on_disk;
      if( ((end - start) % PAGE_SIZE) != 0)
         end = end - (end % PAGE_SIZE);
      if( ((end - start) % PAGE_SIZE) != 0)
         die("File size is wrong (%%PAGE_SIZE!=0)\n");
      if(end == start)
         break;
      int r = pread(fd, cached_data, end - start, start);
      if(r != end - start)
         perr("pread failed! Read %d instead of %lu (offset %lu)\n", r, end-start, start);
      process_existing_chunk(slab_worker_id, s, 1, 0, cached_data, start, end-start, callback);
      start = end;

   }
   free(cached_data);
   s->last_item++;
   rebuild_free_list(s);
}




/*
 * Create a slab: a file that only contains items of a given size.
 * @callback is a callback that will be called on all previously existing items of the slab if it is restored from disk.
 */
struct slab* create_slab(struct slab_context *ctx, int slab_worker_id, size_t item_size, struct slab_callback *callback) {
   struct stat sb;
   char path[512];
   struct slab *s = calloc(1, sizeof(*s));

   size_t disk = slab_worker_id / (get_nb_workers()/get_nb_disks());
   sprintf(path, PATH, disk, slab_worker_id, 0LU, item_size);
   s->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
   if(s->fd == -1)
      perr("Cannot allocate slab %s", path);

   fstat(s->fd, &sb);
   s->size_on_disk = sb.st_size;
   if(s->size_on_disk < 2*PAGE_SIZE) {
      fallocate(s->fd, 0, 0, 2*PAGE_SIZE);
      s->size_on_disk = 2*PAGE_SIZE;
   }

   size_t nb_items_per_page = PAGE_SIZE / item_size;
   s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   s->nb_items = 0;
   s->item_size = item_size;
   s->nb_free_items = 0;
   s->last_item = 0;
   s->ctx = ctx;

   // Read the first page and rebuild the index if the file contains data
   struct item_metadata *meta = read_item(s, 0);
   if(meta->key_size != 0) { // if the key_size is not 0 then then file has been written before
      callback->slab = s;
      rebuild_index(slab_worker_id, s, callback);
   }

   return s;
}

/*
 * Double the size of a slab on disk
 */
struct slab* resize_slab(struct slab *s) {
   if(s->size_on_disk < 10000000000LU) {
      s->size_on_disk *= 2;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items *= 2;
   } else {
      size_t nb_items_per_page = PAGE_SIZE / s->item_size;
      s->size_on_disk += 10000000000LU;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   }
   return s;
}





/*
 * Synchronous read item
 */
void *read_item(struct slab *s, size_t idx) {
   size_t page_num = item_page_num(s, idx);
   char *disk_data = safe_pread(s->fd, page_num*PAGE_SIZE);
   return &disk_data[item_in_page_offset(s, idx)];
}

/*
 * Asynchronous read
 * - read_item_async creates a callback for the ioengine and queues the io request
 * - read_item_async_cb is called when io is completed (might be synchronous if page is cached)
 * If nothing happen it is because do_io is not called.
 */
void read_item_async_cb(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;
   off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
   if(callback->cb)
      callback->cb(callback, &disk_page[in_page_offset]);
}

void read_item_async(struct slab_callback *callback) {
   callback->io_cb = read_item_async_cb;
   read_page_async(callback);
}

/*
 * Asynchronous update item:
 * - First read the page where the item is staying
 * - Once the page is in page cache, write it
 * - Then send the order to flush it.
 */
void update_item_async_cb2(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;
   off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
   if(callback->cb)
      callback->cb(callback, &disk_page[in_page_offset]);
}

void update_item_async_cb1(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;

   struct slab *s = callback->slab;
   size_t idx = callback->slab_idx;
   void *item = callback->item;
   struct item_metadata *meta = item;
   off_t offset_in_page = item_in_page_offset(s, idx);
   struct item_metadata *old_meta = (void*)(&disk_page[offset_in_page]);

   if(callback->action == UPDATE) {
      size_t new_key_size = meta->key_size;
      size_t old_key_size = old_meta->key_size;
      if(new_key_size != old_key_size) {
         die("Updating an item, but key size changed! Likely this is because 2 keys have the same prefix in the index and we got confused because they have the same prefix. TODO: make the index more robust by detecting that 2 keys have the same prefix and transforming the prefix -> slab_idx to prefix -> [ { full key 1, slab_idx1 }, { full key 2, slab_idx2 } ]\n");
      }

      char *new_key = &disk_page[offset_in_page + sizeof(*meta)];
      char *old_key = &(((char*)old_meta)[sizeof(*meta)]);
      if(memcmp(new_key, old_key, new_key_size))
         die("Updating an item, but key mismatch! Likely this is because 2 keys have the same prefix in the index. TODO: make the index more robust by detecting that 2 keys have the same prefix and transforming the prefix -> slab_idx to prefix -> [ { full key 1, slab_idx1 }, { full key 2, slab_idx2 } ]\n");
   }

   meta->rdt = get_rdt(s->ctx);
   if(meta->key_size == -1)
      memcpy(&disk_page[offset_in_page], meta, sizeof(*meta));
   else if(get_item_size(item) > s->item_size)
      die("Trying to write an item that is too big for its slab\n");
   else
      memcpy(&disk_page[offset_in_page], item, get_item_size(item));

   callback->io_cb = update_item_async_cb2;
   write_page_async(callback);
}

void update_item_async(struct slab_callback *callback) {
   callback->io_cb = update_item_async_cb1;
   read_page_async(callback);
}

/*
 * Add an item is just like updating, but we need to find a suitable page first!
 * get_free_item_idx returns lru_entry == NULL if no page with empty spot exist.
 * If a page with an empty spot exists, we have to scan it to find a suitable spot.
 */
void add_item_async_cb1(struct slab_callback *callback) {
   struct slab *s = callback->slab;

   struct lru *lru_entry = callback->lru_entry;
   if(lru_entry == NULL) { // no free page, append
      if(s->last_item >= s->nb_max_items)
         resize_slab(s);
      callback->slab_idx = s->last_item;
      assert(s->last_item < s->nb_max_items);
      s->last_item++;
   } else { // reuse a free spot. Don't forget to add the linked tombstone in the freelist.
      char *disk_page = callback->lru_entry->page;
      off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
      add_son_in_freelist(callback->slab, callback->slab_idx, (void*)(&disk_page[in_page_offset]));
   }
   s->nb_items++;

   update_item_async(callback);
}

void add_item_async(struct slab_callback *callback) {
   callback->io_cb = add_item_async_cb1;
   get_free_item_idx(callback);
}


/*
 * Remove an item
 */
void remove_item_by_idx_async_cb1(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;

   struct slab *s = callback->slab;
   size_t idx = callback->slab_idx;

   off_t offset_in_page = item_in_page_offset(s, idx);

   struct item_metadata *meta = (void*)&disk_page[offset_in_page];
   if(meta->key_size == -1) { // already removed
      if(callback->cb)
         callback->cb(callback, &disk_page[offset_in_page]);
      return;
   }

   meta->rdt = get_rdt(s->ctx);
   meta->key_size = -1;

   s->nb_items--;
   add_item_in_free_list(s, idx, meta);

   callback->io_cb = update_item_async_cb2;
   write_page_async(callback);
}


void remove_item_async(struct slab_callback *callback) {
   callback->io_cb = remove_item_by_idx_async_cb1;
   read_page_async(callback);
}
