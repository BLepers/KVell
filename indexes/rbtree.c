/* Copyright (c) 2011 the authors listed at the following URL, and/or
   the authors of referenced articles or incorporated external code:
http://en.literateprograms.org/Red-black_tree_(C)?action=history&offset=20090121005050

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Retrieved from: http://en.literateprograms.org/Red-black_tree_(C)?oldid=16016
*/

#include "rbtree.h"
#include <assert.h>

#include <stdlib.h>


typedef rbtree_node node;
typedef enum rbtree_node_color color;


static node grandparent(node n);
static node sibling(node n);
static node uncle(node n);
static void verify_properties(rbtree t);
static color node_color(node n);

#ifdef VERIFY_RBTREE
static void verify_property_1(node root);
static void verify_property_2(node root);
static void verify_property_4(node root);
static void verify_property_5(node root);
static void verify_property_5_helper(node n, int black_count, int* black_count_path);
#endif

static node new_node(void* key, index_entry_t* value, color node_color, node left, node right);
static node lookup_node(rbtree t, void* key, compare_func compare);
static void rotate_left(rbtree t, node n);
static void rotate_right(rbtree t, node n);

static void replace_node(rbtree t, node oldn, node newn);
static void insert_case1(rbtree t, node n);
static void insert_case2(rbtree t, node n);
static void insert_case3(rbtree t, node n);
static void insert_case4(rbtree t, node n);
static void insert_case5(rbtree t, node n);
static node maximum_node(node root);
static void delete_case1(rbtree t, node n);
static void delete_case2(rbtree t, node n);
static void delete_case3(rbtree t, node n);
static void delete_case4(rbtree t, node n);
static void delete_case5(rbtree t, node n);
static void delete_case6(rbtree t, node n);

int pointer_cmp(void *left, void *right) {
   if(left > right) {
      return 1;
   } else if(left < right) {
      return -1;
   } else if(left == right) {
      return 0;
   }
   return 0; //Pleases GCC
}

node grandparent(node n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Not the root node */
   assert (n->parent->parent != NULL); /* Not child of root */
   return n->parent->parent;
}

node sibling(node n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Root node has no sibling */
   if (n == n->parent->left)
      return n->parent->right;
   else
      return n->parent->left;
}

node uncle(node n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Root node has no uncle */
   assert (n->parent->parent != NULL); /* Children of root have no uncle */
   return sibling(n->parent);
}

void verify_properties(rbtree t) {
#ifdef VERIFY_RBTREE
   verify_property_1(t->root);
   verify_property_2(t->root);
   /* Property 3 is implicit */
   verify_property_4(t->root);
   verify_property_5(t->root);
#endif
}

color node_color(node n) {
   return n == NULL ? BLACK : n->color;
}

#ifdef VERIFY_RBTREE
void verify_property_1(node n) {
   assert(node_color(n) == RED || node_color(n) == BLACK);
   if (n == NULL) return;
   verify_property_1(n->left);
   verify_property_1(n->right);
}

void verify_property_2(node root) {
   assert(node_color(root) == BLACK);
}

void verify_property_4(node n) {
   if (node_color(n) == RED) {
      assert (node_color(n->left)   == BLACK);
      assert (node_color(n->right)  == BLACK);
      assert (node_color(n->parent) == BLACK);
   }
   if (n == NULL) return;
   verify_property_4(n->left);
   verify_property_4(n->right);
}

void verify_property_5(node root) {
   int black_count_path = -1;
   verify_property_5_helper(root, 0, &black_count_path);
}

void verify_property_5_helper(node n, int black_count, int* path_black_count) {
   if (node_color(n) == BLACK) {
      black_count++;
   }
   if (n == NULL) {
      if (*path_black_count == -1) {
         *path_black_count = black_count;
      } else {
         assert (black_count == *path_black_count);
      }
      return;
   }
   verify_property_5_helper(n->left,  black_count, path_black_count);
   verify_property_5_helper(n->right, black_count, path_black_count);
}
#endif

rbtree rbtree_create() {
   rbtree t = malloc(sizeof(struct rbtree_t));
   t->root = NULL;
   t->last_visited_node = NULL;
   t->nb_elements = 0;
   verify_properties(t);
   return t;
}

node new_node(void* key, index_entry_t* value, color node_color, node left, node right) {
   node result = malloc(sizeof(struct rbtree_node_t));
   result->key = key;
   result->value = *value;
   result->color = node_color;
   result->left = left;
   result->right = right;
   if (left  != NULL)  left->parent = result;
   if (right != NULL) right->parent = result;
   result->parent = NULL;
   return result;
}

node lookup_node(rbtree t, void* key, compare_func compare) {
   node n = t->root;
   while (n != NULL) {
      int comp_result = compare(key, n->key);
      if (comp_result == 0) {
         t->last_visited_node = n;
         return n;
      } else if (comp_result < 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }
   return n;
}

node lookup_closest_node(rbtree t, void* key, compare_func compare) {
   node n = t->root;
   node closest = NULL;
   while (n != NULL) {
      int comp_result = compare(key, n->key);
      if (comp_result == 0) {
         t->last_visited_node = n;
         return n;
      } else if (comp_result < 0) {
         closest = n;
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }
   return closest;
}

index_entry_t* rbtree_lookup(rbtree t, void* key, compare_func compare) {
   node n = lookup_node(t, key, compare);
   return n == NULL ? NULL : &n->value;
}

void rotate_left(rbtree t, node n) {
   node r = n->right;
   replace_node(t, n, r);
   n->right = r->left;
   if (r->left != NULL) {
      r->left->parent = n;
   }
   r->left = n;
   n->parent = r;
}

void rotate_right(rbtree t, node n) {
   node L = n->left;
   replace_node(t, n, L);
   n->left = L->right;
   if (L->right != NULL) {
      L->right->parent = n;
   }
   L->right = n;
   n->parent = L;
}

void replace_node(rbtree t, node oldn, node newn) {
   if (oldn->parent == NULL) {
      t->root = newn;
   } else {
      if (oldn == oldn->parent->left)
         oldn->parent->left = newn;
      else
         oldn->parent->right = newn;
   }
   if (newn != NULL) {
      newn->parent = oldn->parent;
   }
}

void rbtree_insert(rbtree t, void* key, index_entry_t* value, compare_func compare) {
   node inserted_node = new_node(key, value, RED, NULL, NULL);
   /* Classic hack to speed up the find & insert case */
   if (t->last_visited_node && compare(key, t->last_visited_node->key) == 0) {
      t->last_visited_node->value = *value;
      free(inserted_node);
      return;
   } else if (t->root == NULL) {
      t->root = inserted_node;
      t->nb_elements = 1;
   } else {
      node n = t->root;
      while (1) {
         int comp_result = compare(key, n->key);
         if (comp_result == 0) {
            n->value = *value;
            /* inserted_node isn't going to be used, don't leak it */
            free (inserted_node);
            return;
         } else if (comp_result < 0) {
            if (n->left == NULL) {
               n->left = inserted_node;
               t->nb_elements++;
               break;
            } else {
               n = n->left;
            }
         } else {
            assert (comp_result > 0);
            if (n->right == NULL) {
               n->right = inserted_node;
               t->nb_elements++;
               break;
            } else {
               n = n->right;
            }
         }
      }
      inserted_node->parent = n;
   }
   insert_case1(t, inserted_node);
   verify_properties(t);
}

void insert_case1(rbtree t, node n) {
   if (n->parent == NULL)
      n->color = BLACK;
   else
      insert_case2(t, n);
}

void insert_case2(rbtree t, node n) {
   if (node_color(n->parent) == BLACK)
      return; /* Tree is still valid */
   else
      insert_case3(t, n);
}

void insert_case3(rbtree t, node n) {
   if (node_color(uncle(n)) == RED) {
      n->parent->color = BLACK;
      uncle(n)->color = BLACK;
      grandparent(n)->color = RED;
      insert_case1(t, grandparent(n));
   } else {
      insert_case4(t, n);
   }
}

void insert_case4(rbtree t, node n) {
   if (n == n->parent->right && n->parent == grandparent(n)->left) {
      rotate_left(t, n->parent);
      n = n->left;
   } else if (n == n->parent->left && n->parent == grandparent(n)->right) {
      rotate_right(t, n->parent);
      n = n->right;
   }
   insert_case5(t, n);
}

void insert_case5(rbtree t, node n) {
   n->parent->color = BLACK;
   grandparent(n)->color = RED;
   if (n == n->parent->left && n->parent == grandparent(n)->left) {
      rotate_right(t, grandparent(n));
   } else {
      assert (n == n->parent->right && n->parent == grandparent(n)->right);
      rotate_left(t, grandparent(n));
   }
}

void rbtree_delete(rbtree t, void* key, compare_func compare) {
   node child;
   node n = lookup_node(t, key, compare);
   if (n == NULL) return;  /* Key not found, do nothing */
   t->nb_elements--;
   if (n->left != NULL && n->right != NULL) {
      /* Copy key/value from predecessor and then delete it instead */
      node pred = maximum_node(n->left);
      n->key   = pred->key;
      n->value = pred->value;
      n = pred;
   }

   assert(n->left == NULL || n->right == NULL);
   child = n->right == NULL ? n->left  : n->right;
   if (node_color(n) == BLACK) {
      n->color = node_color(child);
      delete_case1(t, n);
   }
   replace_node(t, n, child);
   if (n->parent == NULL && child != NULL)
      child->color = BLACK;
   free(n);

   verify_properties(t);

   t->last_visited_node = NULL;
}

static node maximum_node(node n) {
   assert (n != NULL);
   while (n->right != NULL) {
      n = n->right;
   }
   return n;
}

void delete_case1(rbtree t, node n) {
   if (n->parent == NULL)
      return;
   else
      delete_case2(t, n);
}

void delete_case2(rbtree t, node n) {
   if (node_color(sibling(n)) == RED) {
      n->parent->color = RED;
      sibling(n)->color = BLACK;
      if (n == n->parent->left)
         rotate_left(t, n->parent);
      else
         rotate_right(t, n->parent);
   }
   delete_case3(t, n);
}

void delete_case3(rbtree t, node n) {
   if (node_color(n->parent) == BLACK &&
         node_color(sibling(n)) == BLACK &&
         node_color(sibling(n)->left) == BLACK &&
         node_color(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      delete_case1(t, n->parent);
   }
   else
      delete_case4(t, n);
}

void delete_case4(rbtree t, node n) {
   if (node_color(n->parent) == RED &&
         node_color(sibling(n)) == BLACK &&
         node_color(sibling(n)->left) == BLACK &&
         node_color(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      n->parent->color = BLACK;
   }
   else
      delete_case5(t, n);
}

void delete_case5(rbtree t, node n) {
   if (n == n->parent->left &&
         node_color(sibling(n)) == BLACK &&
         node_color(sibling(n)->left) == RED &&
         node_color(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      sibling(n)->left->color = BLACK;
      rotate_right(t, sibling(n));
   }
   else if (n == n->parent->right &&
         node_color(sibling(n)) == BLACK &&
         node_color(sibling(n)->right) == RED &&
         node_color(sibling(n)->left) == BLACK)
   {
      sibling(n)->color = RED;
      sibling(n)->right->color = BLACK;
      rotate_left(t, sibling(n));
   }
   delete_case6(t, n);
}

void delete_case6(rbtree t, node n) {
   sibling(n)->color = node_color(n->parent);
   n->parent->color = BLACK;
   if (n == n->parent->left) {
      assert (node_color(sibling(n)->right) == RED);
      sibling(n)->right->color = BLACK;
      rotate_left(t, n->parent);
   }
   else
   {
      assert (node_color(sibling(n)->left) == RED);
      sibling(n)->left->color = BLACK;
      rotate_right(t, n->parent);
   }
}

void rbtree_print_nodes(node n, compare_func show) {
   if(!n)
      return;
   rbtree_print_nodes(n->left, show);
   show(n->key, &n->value);
   rbtree_print_nodes(n->right, show);
}
void rbtree_print(rbtree t, compare_func show) {
   node n = t->root;
   rbtree_print_nodes(n, show);
}


void rbtree_fill_scan(rbtree t, node n, size_t max, struct rbtree_scan_tmp *res) {
   if(!n)
      return;
   if(n->left)
      rbtree_fill_scan(t, n->left, max, res);
   if(res->nb_entries < max) {
      res->entries[res->nb_entries] = *n;
      res->nb_entries++;
   }
   if(res->nb_entries < max)
      rbtree_fill_scan(t, n->right, max, res);
}

void rbtree_fill_scan_up(rbtree t, node n, size_t max, struct rbtree_scan_tmp *res) {
   if(res->nb_entries < max) {
      node parent = n->parent;
      if(!parent)
         return;
      if(parent->left == n) {
         res->entries[res->nb_entries] = *parent;
         res->nb_entries++;
         if(res->nb_entries < max)
            rbtree_fill_scan(t, parent->right, max, res);
      }
      rbtree_fill_scan_up(t, parent, max, res);
   }
}

struct rbtree_scan_tmp rbtree_lookup_n(rbtree t, void *key, size_t n, compare_func compare) {
   struct rbtree_scan_tmp res;
   res.entries = malloc(n * sizeof(*res.entries));
   res.nb_entries = 0;

   node start = lookup_closest_node(t, key, compare);
   if(start == NULL)
      return res;
   res.entries[0] = *start;
   res.nb_entries++;

   rbtree_fill_scan(t, start->right, n, &res);
   rbtree_fill_scan_up(t, start, n, &res);

   return res;
}
