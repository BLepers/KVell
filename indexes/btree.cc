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

   int btree_find(btree_t *t, unsigned char* k, size_t len, struct index_entry *e) {
      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find(hash);
      if(i != b->end()) {
         *e = i->second;
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

   struct index_scan btree_find_n(btree_t *t, unsigned char* k, size_t len, size_t n) {
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      uint64_t hash = *(uint64_t*)k;
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find_closest(hash);
      while(i != b->end() && res.nb_entries < n) {
         res.hashes[res.nb_entries] = i->first;
         res.entries[res.nb_entries] = i->second;
         res.nb_entries++;
         i++;
      }

      return res;
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
