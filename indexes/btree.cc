#include "cpp-btree/btree_map.h"
#include "btree.h"

using namespace std;
using namespace btree;

extern "C"
{
   btree_t *btree_create() {
      btree_map<uint64_t, struct index_entry> *b = new btree_map<uint64_t, struct index_entry>();
      return b;
   }

   size_t btree_size(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      return b->size();
   }

   int btree_find(btree_t *t, unsigned char* k, size_t len, struct index_entry **e) {
      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find(hash);
      if(i != b->end()) {
         *e = &i->second;
         return 1;
      } else {
         *e = NULL;
         return 0;
      }
   }


   int btree_find_next(btree_t *t, unsigned char* k, size_t len, struct index_entry **e, uint64_t *found_hash) {
      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find_closest(hash);
      *e = NULL;
redo:
      if(i != b->end()) {
         if(i->first == hash) { // find_closest returned an exact match, but we want the next element
            i++;
            goto redo;
         } else {
            *e = &i->second;
            *found_hash = i->first;
         }
         return 1;
      } else {
         return 0;
      }
   }

   void btree_delete(btree_t *t, unsigned char*k, size_t len) {
      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      b->erase(hash);
   }

   void btree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e) {
      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      b->insert(make_pair(hash, *e));
   }

   void btree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->begin();
      while(i != b->end()) {
         cb(i->first, data);
         i++;
      }
      return;
   }


   void btree_free(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      delete b;
   }
}
