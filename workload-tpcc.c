/*
 * TPCC workload
 */

#include "headers.h"
#include "workload-common.h"
#include "workload-tpcc.h"

/*
 * We reuse the format of redisdriver.py from py-tpcc
 * File contains:
 * 1/ Definitions of tables and columns
 * 2/ Functions to generate keys for items stored in the KV (e.g., a unique key for an item or a warehouse)
 * 3/ Loader functions
 * 4/ Queries
 */

/*
 * TPCC has a few tables and columns:
 */
enum table { WAREHOUSE, DISTRICT, ITEM, CUSTOMER, HISTORY, STOCK, ORDERS, NEW_ORDER, ORDER_LINE, CUSTOMER_INDEXES, ORDERS_INDEXES };

enum column {
   W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY , W_STATE, W_ZIP, W_TAX, W_YTD,
   D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID,
   I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA,
   C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA,
   H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA,
   S_I_ID, S_W_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA,
   O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL,
   NO_O_ID, NO_D_ID, NO_W_ID,
   OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO,
   CUSTOMER_INDEXES_KEY, // these are here to emulate secondary indexes, e.g., customer_name => customer (primary index is customer_id => customer)
   ORDERS_INDEXES_KEY,
   NEW_ORDER_INDEXES_KEY,
   ORDER_LINE_INDEXES_KEY,
};


/*
 * We want every DB insert to have a unique and identifiable key. These functions generate a unique key depending on the table, column and primary key(s) of a tuple.
 */
struct tpcc_key {
   union {
      long key;
      struct {
         unsigned long prim_key:60;
         unsigned int table:4;
      };
   };
} __attribute__((packed));

static long get_key_warehouse(long warehouse) {
   struct tpcc_key k = {
      .table = WAREHOUSE,
      .prim_key = warehouse
   };
   return k.key;
}

static long get_key_district(long warehouse, long district) {
   struct tpcc_key k = {
      .table = DISTRICT,
      .prim_key = district + (warehouse << 7)
   };
   return k.key;
}

static long get_key_customer(long warehouse, long district, long customer) {
   struct tpcc_key k = {
      .table = CUSTOMER,
      .prim_key = customer + (district << 32) + (warehouse << 39)
   };
   return k.key;
}

static long get_key_customer_indexes(long warehouse, long district, long customer) {
   struct tpcc_key k = {
      .table = CUSTOMER_INDEXES,
      .prim_key = customer + (district << 32) + (warehouse << 39)
   };
   return k.key;
}

static long get_key_history(long uid) {
   struct tpcc_key k = {
      .table = HISTORY,
      .prim_key = uid,
   };
   return k.key;
}

static long get_key_order(long warehouse, long district, long order) {
   struct tpcc_key k = {
      .table = ORDERS,
      .prim_key = order + (district << 32) + (warehouse << 39)
   };
   return k.key;
}

static long get_key_order_index(long warehouse, long district, long client) {
   struct tpcc_key k = {
      .table = ORDERS_INDEXES,
      .prim_key = client + (district << 32) + (warehouse << 39)
   };
   return k.key;
}


static long get_key_new_order(long warehouse, long district, long order) {
   struct tpcc_key k = {
      .table = NEW_ORDER,
      .prim_key = order + (district << 32) + (warehouse << 39)
   };
   return k.key;
}

static long get_key_order_line(long warehouse, long district, long ol_o_id, long ol_number) {
   struct tpcc_key k = {
      .table = ORDER_LINE,
      .prim_key = ol_number + (ol_o_id << 20) + (district << 40) + (warehouse << 47)
   };
   return k.key;
}

static long get_key_stock(long warehouse, long item) {
   struct tpcc_key k = {
      .table = STOCK,
      .prim_key = item + (warehouse << 32)
   };
   return k.key;
}

static long get_key_item(long uid) {
   struct tpcc_key k = {
      .table = ITEM,
      .prim_key = uid
   };
   return k.key;
}

static enum table get_table_from_item(char *item) {
   struct tpcc_key *k = (void*)&item[sizeof(struct item_metadata)];
   return k->table;
}

/*
 * TPCC loader
 * Generate all the elements that will be stored in the DB
 */

/* 'WAREHOUSE' elements */
static char *tpcc_warehouse(uint64_t uid) {
   uint64_t key = get_key_warehouse(uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, W_ID, uid, 1);
   item = add_shash_string(item, W_NAME, MIN_NAME, MAX_NAME, 1);
   item = add_shash_string(item, W_STREET_1, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, W_STREET_2, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, W_CITY, MIN_CITY, MAX_CITY, 1);
   item = add_shash_string(item, W_STATE, 2, 2, 1);
   item = add_shash_string(item, W_ZIP, 9, 9, 1);
   item = add_shash_float(item, W_TAX, rand_float_between(MIN_TAX, MAX_TAX), 1);
   item = add_shash_uint(item, W_YTD, INITIAL_W_YTD, 1);

   return item;
}

/* 'DISTRICTS' elements */
static char *tpcc_district(uint64_t warehouse, uint64_t uid) {
   uint64_t key = get_key_district(warehouse, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, D_ID, uid, 1);
   item = add_shash_uint(item, D_W_ID, warehouse, 1);
   item = add_shash_string(item, D_NAME, MIN_NAME, MAX_NAME, 1);
   item = add_shash_string(item, D_STREET_1, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, D_STREET_2, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, D_CITY, MIN_CITY, MAX_CITY, 1);
   item = add_shash_string(item, D_STATE, 2, 2, 1);
   item = add_shash_string(item, D_ZIP, 9, 9, 1);
   item = add_shash_float(item, D_TAX, rand_float_between(MIN_TAX, MAX_TAX), 1);
   item = add_shash_uint(item, D_YTD, INITIAL_W_YTD, 1);
   item = add_shash_uint(item, D_NEXT_O_ID, CUSTOMERS_PER_DISTRICT, 1);

   return item;
}

/* 'CUSTOMER' */
static char *tpcc_customer(uint64_t warehouse, uint64_t district, uint64_t uid) {
   uint64_t key = get_key_customer(warehouse, district, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, C_ID, uid, 1);
   item = add_shash_uint(item, C_D_ID, district, 1);
   item = add_shash_uint(item, C_W_ID, warehouse, 1);
   item = add_shash_string(item, C_FIRST, MIN_FIRST, MAX_FIRST, 1);
   item = add_shash_element(item, C_MIDDLE, MIDDLE, sizeof(MIDDLE), 1);
   item = add_shash_uint(item, C_LAST, uid, 1); // We use the uid as last name because it doesn't change performance much but simplifies generating existing last names A LOT

   item = add_shash_string(item, C_STREET_1, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, C_STREET_2, MIN_STREET, MAX_STREET, 1);
   item = add_shash_string(item, C_CITY, MIN_CITY, MAX_CITY, 1);
   item = add_shash_string(item, C_STATE, 2, 2, 1);
   item = add_shash_string(item, C_ZIP, 9, 9, 1);

   item = add_shash_string(item, C_PHONE, PHONE, PHONE, 1);
   item = add_shash_uint(item, C_SINCE, 0, 1); // Fix date()
   if(rand_between(0,100) < 10) {
      item = add_shash_element(item, C_CREDIT, BAD_CREDIT, sizeof(BAD_CREDIT), 1);
   } else {
      item = add_shash_element(item, C_CREDIT, GOOD_CREDIT, sizeof(GOOD_CREDIT), 1);
   }
   item = add_shash_uint(item, C_CREDIT_LIM, INITIAL_CREDIT_LIM, 1);
   item = add_shash_float(item, C_DISCOUNT, rand_float_between(MIN_DISCOUNT, MAX_DISCOUNT), 1);
   item = add_shash_float(item, C_BALANCE, INITIAL_BALANCE, 1);
   item = add_shash_float(item, C_YTD_PAYMENT, INITIAL_YTD_PAYMENT, 1);
   item = add_shash_float(item, C_PAYMENT_CNT, INITIAL_PAYMENT_CNT, 1);
   item = add_shash_uint(item, C_DELIVERY_CNT, INITIAL_DELIVERY_CNT, 1);
   item = add_shash_string(item, C_DATA, MIN_C_DATA, MAX_C_DATA, 1);

   return item;
}

/* Index of all customers, by last name */
static char *tpcc_customer_index(uint64_t warehouse, uint64_t district, uint64_t uid) {
   uint64_t key = get_key_customer_indexes(warehouse, district, uid);
   char *item = create_uint_value(key, get_key_customer(warehouse, district, uid));
   return item;
}

/* 'HISTORY' */
static char *tpcc_history(uint64_t uid) {
   uint64_t key = get_key_history(uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, H_C_ID, 0, 1); // we don't care about the content, it is never read... and we don't do compression, so no cheating
   item = add_shash_uint(item, H_C_D_ID, 0, 1);
   item = add_shash_uint(item, H_C_W_ID, 0, 1);
   item = add_shash_uint(item, H_D_ID, 0, 1);
   item = add_shash_uint(item, H_W_ID, 0, 1);
   item = add_shash_uint(item, H_DATE, 0, 1);
   item = add_shash_uint(item, H_AMOUNT, 0, 1);
   item = add_shash_string(item, H_DATA, MIN_DATA, MAX_DATA, 1);

   return item;
}


/* 'ORDER' */
static char *tpcc_order(uint64_t warehouse, uint64_t district, uint64_t client, uint64_t uid, uint64_t nb_order_line) {
   uint64_t key = get_key_order(warehouse, district, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, O_ID, uid, 1);
   item = add_shash_uint(item, O_D_ID, district, 1);
   item = add_shash_uint(item, O_W_ID, warehouse, 1);
   item = add_shash_uint(item, O_C_ID, client, 1);
   item = add_shash_uint(item, O_ENTRY_D, 0, 1); // Fix date()
   item = add_shash_uint(item, O_CARRIER_ID, NULL_CARRIER_ID, 1); // Fix constants.NULL_CARRIER_ID if newOrder else rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
   item = add_shash_uint(item, O_OL_CNT, nb_order_line, 1);
   item = add_shash_uint(item, O_ALL_LOCAL, INITIAL_ALL_LOCAL, 1);

   return item;
}

/* 'ORDER' */
static char *tpcc_order_index(uint64_t warehouse, uint64_t district, uint64_t client, uint64_t uid) {
   uint64_t key = get_key_order_index(warehouse, district, client);

   char *item = create_smember(key);
   item = add_smember(item, uid, 1);
   return item;
}


/* 'NEW_ORDER' */
static char *tpcc_new_order(uint64_t warehouse, uint64_t district, uint64_t uid) {
   uint64_t key = get_key_new_order(warehouse, district, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, NO_O_ID, uid, 1);
   item = add_shash_uint(item, NO_D_ID, district, 1);
   item = add_shash_uint(item, NO_W_ID, warehouse, 1);

   return item;
}

/* 'ORDER LINE' */
static char *tpcc_order_line(uint64_t warehouse, uint64_t district, uint64_t order, uint64_t uid) {
   uint64_t key = get_key_order_line(warehouse, district, order, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, OL_O_ID, order, 1);
   item = add_shash_uint(item, OL_D_ID, district, 1);
   item = add_shash_uint(item, OL_W_ID, warehouse, 1);
   item = add_shash_uint(item, OL_NUMBER, uid, 1);
   item = add_shash_uint(item, OL_I_ID, rand_between(0, NUM_ITEMS), 1); // should be unique per order line...
   item = add_shash_uint(item, OL_SUPPLY_W_ID, warehouse, 1); // Fix, 1% are remote
   item = add_shash_uint(item, OL_DELIVERY_D, 0, 1);
   item = add_shash_uint(item, OL_QUANTITY, INITIAL_QUANTITY, 1);
   item = add_shash_uint(item, OL_AMOUNT, rand_between(MIN_PRICE, MAX_PRICE), 1);

   // Fix missing fields
   return item;
}

/* 'STOCK' */
static char *tpcc_stock(uint64_t warehouse, uint64_t uid) {
   uint64_t key = get_key_stock(warehouse, uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, S_I_ID, uid, 1);
   item = add_shash_uint(item, S_W_ID, warehouse, 1);
   item = add_shash_uint(item, S_QUANTITY, rand_between(MIN_QUANTITY, MAX_QUANTITY), 1);

   return item;
}

/* 'ITEMS' elements */
static char *tpcc_item(uint64_t uid) {
   uint64_t key = get_key_item(uid);

   char *item = create_shash(key);
   item = add_shash_uint(item, I_ID, uid, 1);
   item = add_shash_uint(item, I_IM_ID, rand_between(MIN_IM, MAX_IM), 1);
   item = add_shash_string(item, I_NAME, MIN_I_NAME, MAX_I_NAME, 1);
   item = add_shash_float(item, I_PRICE, rand_float_between(MIN_PRICE, MAX_PRICE), 1);
   item = add_shash_string(item, I_DATA, MIN_I_DATA, MAX_I_DATA, 1);

   return item;
}

/*
 * Function called by the workload-common interface. uid goes from 0 to the number of elements in the DB.
 * For this to work, max_uid should be == get_db_size_tpcc()
 */
static char *create_unique_item_tpcc(uint64_t uid, uint64_t max_uid) {
   if(uid == 0)
      assert(max_uid == get_db_size_tpcc()); // set .nb_items_in_db = get_db_size_tpcc() in main.c otherwise the DB will be partially populated

   if(uid < NB_WAREHOUSES)
      return tpcc_warehouse(uid);
   uid -= NB_WAREHOUSES;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE) {
      uint64_t district = uid % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = uid / DISTRICTS_PER_WAREHOUSE;
      return tpcc_district(warehouse, district);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) {
      uint64_t customer = uid % CUSTOMERS_PER_DISTRICT;
      uint64_t district = (uid / CUSTOMERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / CUSTOMERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_customer(warehouse, district, customer);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) {
      uint64_t customer = uid % CUSTOMERS_PER_DISTRICT;
      uint64_t district = (uid / CUSTOMERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / CUSTOMERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_customer_index(warehouse, district, customer);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) {
      return tpcc_history(uid);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) {
      uint64_t order = uid % CUSTOMERS_PER_DISTRICT;
      uint64_t client = uid % CUSTOMERS_PER_DISTRICT; // every client has 1 order, and for simplicity client_id == order_id, not that it matters much...
      uint64_t district = (uid / CUSTOMERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / CUSTOMERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_order(warehouse, district, client, order, AVG_OL_CNT);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) {
      uint64_t order = uid % CUSTOMERS_PER_DISTRICT;
      uint64_t client = uid % CUSTOMERS_PER_DISTRICT; // every client has 1 order, and for simplicity client_id == order_id, not that it matters much...
      uint64_t district = (uid / CUSTOMERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / CUSTOMERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_order_index(warehouse, district, client, order);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * INITIAL_NEW_ORDERS_PER_DISTRICT) {
      uint64_t order = uid % INITIAL_NEW_ORDERS_PER_DISTRICT; // must be equal to client in tpcc_order, see before
      uint64_t district = (uid / INITIAL_NEW_ORDERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / INITIAL_NEW_ORDERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_new_order(warehouse, district, order);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * INITIAL_NEW_ORDERS_PER_DISTRICT;

   if(uid < NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT * AVG_OL_CNT) {
      uint64_t order_line = uid % AVG_OL_CNT;
      uint64_t order = (uid / AVG_OL_CNT) % CUSTOMERS_PER_DISTRICT;
      uint64_t district = (uid / AVG_OL_CNT / CUSTOMERS_PER_DISTRICT) % DISTRICTS_PER_WAREHOUSE;
      uint64_t warehouse = (uid / AVG_OL_CNT / CUSTOMERS_PER_DISTRICT) / DISTRICTS_PER_WAREHOUSE;
      return tpcc_order_line(warehouse, district, order, order_line);
   }
   uid -= NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT * AVG_OL_CNT;

   if(uid < NB_WAREHOUSES * NUM_ITEMS) {
      uint64_t item = uid % NUM_ITEMS;
      uint64_t warehouse = (uid / NUM_ITEMS);
      return tpcc_stock(warehouse, item);
   }
   uid -= NB_WAREHOUSES * NUM_ITEMS;

   return tpcc_item(uid);
}


size_t get_db_size_tpcc(void) {
   return NB_WAREHOUSES                                                                   // warehouses
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE                                           // districts
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT                  // customers
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT                  // customers index
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT                  // history
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT                  // orders
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT                  // orders index
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * INITIAL_NEW_ORDERS_PER_DISTRICT         // new orders
      + NB_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT * AVG_OL_CNT     // order lines
      + NB_WAREHOUSES * NUM_ITEMS                                                         // stock
      + NUM_ITEMS;
}


/*
 * Generic callback helpers. Allow to automatically perform the multiple steps of a TPCC query without dealing too much with asynchrony.
 */
struct transaction_payload {
   uint64_t query_id;
   uint64_t nb_requests_to_do, completed_requests; // when completed_requests == nb_requests_to_do, commit transaction
};

static volatile uint64_t failed_trans, nb_commits_done;
static void commit_cb(struct slab_callback *cb, void *item) {
   uint64_t end;
   rdtscll(end);
   add_timing_stat(end - get_start_time(cb->transaction));

   __sync_fetch_and_add(&nb_commits_done, 1);
   if(has_failed(cb->transaction))
      __sync_fetch_and_add(&failed_trans, 1);

   struct transaction_payload *p = get_payload(cb->transaction);
   free(p);
   free(cb);
}

static void compute_stats_tpcc(struct slab_callback *cb, void *item) {
   struct transaction_payload *p = get_payload(cb->transaction);

   slab_cb_t *next = cb->payload2;
   if(!has_failed(cb->transaction)) {
      next(cb, item);
   } else {
      free(cb->payload);
   }

   p->completed_requests++;
   if(p->completed_requests == p->nb_requests_to_do) { // Time to commit
      struct slab_callback *new_cb = new_slab_callback();
      new_cb->cb = commit_cb;
      new_cb->transaction = cb->transaction;
      new_cb->injector_queue = cb->injector_queue;
      kv_commit(cb->transaction, new_cb);
   }

   free(cb->item);
   free(cb);
}

static struct transaction_payload *get_tpcc_payload(struct transaction *t) {
   return get_payload(t);
}

static struct slab_callback *tpcc_cb(struct injector_queue *q, struct transaction *t, void *payload, slab_cb_t *next) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = compute_stats_tpcc;
   cb->injector_queue = q;
   cb->payload = payload;
   cb->payload2 = next;
   get_tpcc_payload(t)->nb_requests_to_do++;
   return cb;
}

/*
 * Helpers to generate random queries
 */
static long get_rand_warehouse(void) {
   return uniform_next() % NB_WAREHOUSES;
}

static long get_rand_district(void) {
   return uniform_next() % DISTRICTS_PER_WAREHOUSE;
}

static long get_rand_customer(void) {
   return zipf_next() % CUSTOMERS_PER_DISTRICT;
}


/*
 * Order status query
 */
struct status_payload {
   long warehouse;
   long district;
   long customer;
   long oid;
};

static void _tpcc_status_4(struct slab_callback *cb, void *item) {
   struct status_payload *p = cb->payload;
   free(p);
}

static void _tpcc_status_3(struct slab_callback *cb, void *item) {
   struct status_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDER\n");

   uint64_t nb_elements = get_shash_uint(item, O_OL_CNT);
   for(size_t i = 0; i < nb_elements; i++) {
      struct status_payload *new_payload = malloc(sizeof(*p));
      new_payload->warehouse = p->warehouse;
      new_payload->district = p->district;
      new_payload->customer = p->customer;
      new_payload->oid = p->oid;

      uint64_t key_order_line = get_key_order_line(p->warehouse, p->district, p->oid, i);

      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_status_4);
      ncb->item = create_key(key_order_line);
      kv_trans_read(cb->transaction, ncb);
   }

   free(p);
}

static void _tpcc_status_2(struct slab_callback *cb, void *item) {
   struct status_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDERS_INDEXES\n");

   struct smember *s = get_smember(item);
   for(size_t i = 0; i < s->nb_elements; i++) {
      struct status_payload *new_payload = malloc(sizeof(*p));
      new_payload->warehouse = p->warehouse;
      new_payload->district = p->district;
      new_payload->customer = p->customer;
      new_payload->oid = s->elements[i];

      long order_line_key = get_key_order(p->warehouse, p->district, new_payload->oid);
      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_status_3);
      ncb->item = create_key(order_line_key);
      kv_trans_read(cb->transaction, ncb);
   }

   free(p);
}

static void _tpcc_status_1(struct slab_callback *cb, void *item) {
   struct status_payload *p = cb->payload;
   if(!item)
      die("Couldn't find CUSTOMER\n");

   enum table t = get_table_from_item(item);
   if(t == CUSTOMER_INDEXES) { // customer was searched by name, we now have its ID, so perform the actual query to get its data
      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_status_1);
      ncb->item = create_key(get_key_customer(p->warehouse, p->district, p->customer));
      kv_trans_read(cb->transaction, ncb);
      return;
   }

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_status_2);
   ncb->item = create_key(get_key_order_index(p->warehouse, p->district, p->customer));
   kv_trans_read(cb->transaction, ncb);
}

static void tpcc_orderstatus_query(struct injector_queue *q, struct transaction *t) {
   struct status_payload *p = calloc(1, sizeof(*p));
   p->warehouse = get_rand_warehouse();
   p->district = get_rand_district();
   p->customer = get_rand_customer();

   long key;
   struct slab_callback *ncb = tpcc_cb(q, t, p, _tpcc_status_1);
   if(rand_between(0, 100) < 60) { // request by customer name
      key = get_key_customer_indexes(p->warehouse, p->district, p->customer);
   } else {
      key = get_key_customer(p->warehouse, p->district, p->customer);
   }
   ncb->item = create_key(key);
   kv_trans_read(t, ncb);
}



/*
 * Delivery query
 */
struct delivery_payload {
   long warehouse;
   long district;
   long customer;
   long oid;
   long onum;
   long total_completed, total_to_complete;
   long total_sum;
   struct delivery_payload *main_payload;
};

static void _tpcc_delivery_6(struct slab_callback *cb, void *item) {
   struct delivery_payload *p = cb->payload;
   free(p);
}

static void _tpcc_delivery_5(struct slab_callback *cb, void *item) {
   struct delivery_payload *p = cb->payload;
   if(!item)
      die("Couldn't find CUSTOMER.%ld.%ld.%ld\n", p->warehouse, p->district, p->customer);

   char *new_item = clone_item(item);
   incr_shash_uint(new_item, C_BALANCE, p->total_sum);
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_delivery_6);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}

static void _tpcc_delivery_4(struct slab_callback *cb, void *item) {
   struct delivery_payload *p = cb->payload;
   free(p);
}

static void _tpcc_delivery_3(struct slab_callback *cb, void *item) {
   struct delivery_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDER_LINE.%ld.%ld.%ld.%ld\n", p->warehouse, p->district, p->oid, p->onum);

   // main_payload still exists (hasn't been freed in any callback, and the transaction didn't fail yet because we are called, so it hasn't been freed automatically)
   p->main_payload->total_sum += get_shash_uint(item, OL_AMOUNT);
   p->main_payload->total_completed++;

   // We have computed the total cost of the order, now read the client and then update it
   if(p->main_payload->total_completed == p->main_payload->total_to_complete) {
      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p->main_payload, _tpcc_delivery_5);
      ncb->item = create_key(get_key_customer(p->warehouse, p->district, p->customer));
      kv_trans_read(cb->transaction, ncb);
   }

   char *new_item = clone_item(item);
   incr_shash_uint(new_item, OL_DELIVERY_D, 1); // set delivery date to a random value
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_delivery_4);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}

static void _tpcc_delivery_2(struct slab_callback *cb, void *item) {
   // Nothing to do, we updated the order
}

static void _tpcc_delivery_1(struct slab_callback *cb, void *item) {
   struct delivery_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDER.%ld.%ld.%ld\n", p->warehouse, p->district, p->oid);

   p->customer = get_shash_uint(item, O_C_ID);
   p->total_sum = 0;

   /* Set carrier ID in the order */
   char *new_item = clone_item(item);
   incr_shash_uint(new_item, O_CARRIER_ID, 1); // set carrier ID to a random value

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_delivery_2);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);

   /* Change the order lines */
   uint64_t nb_elements = get_shash_uint(item, O_OL_CNT);
   p->total_to_complete = nb_elements;
   p->total_completed = 0;
   for(size_t i = 0; i < nb_elements; i++) {
      struct delivery_payload *new_payload = malloc(sizeof(*p));
      new_payload->warehouse = p->warehouse;
      new_payload->district = p->district;
      new_payload->oid = p->oid;
      new_payload->onum = i;
      new_payload->customer = p->customer;
      new_payload->main_payload = p;

      uint64_t key_order_line = get_key_order_line(p->warehouse, p->district, p->oid, i);

      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_delivery_3);
      ncb->item = create_key(key_order_line);
      kv_trans_read(cb->transaction, ncb);
   }
}

static void tpcc_delivery_query(struct injector_queue *q, struct transaction *t) {
   uint64_t warehouse = get_rand_warehouse();

   for(size_t i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
      struct delivery_payload *p = malloc(sizeof(*p));
      p->warehouse = warehouse;
      p->district = i;

      /*void *item1 = create_key(get_key_new_order(warehouse, i, 0));
      void *item2 = create_key(get_key_new_order(warehouse, i + 1, 0));
      struct index_scan r = memory_index_scan_between(item1, item2, get_snapshot_version(t));
      if(r.nb_entries) {
         size_t random_entry = rand_between(0, r.nb_entries);
         size_t random_new_order_key = r.hashes[random_entry];
         size_t random_order_id = get_order_from_new_order_key(random_new_order_key);
         p->oid = random_order_id;

         struct slab_callback *ncb = tpcch_cb(q, t, p, _tpcch_delivery_1);
         ncb->item = create_key(get_key_order(warehouse, i, random_order_id));
         kv_trans_read(t, ncb);
      } else {
         free(p);
      }
      free(item1);
      free(item2);
      free(r.hashes);
      free(r.entries);*/

      // TODO: reimplement the scan!!
      struct slab_callback *ncb = tpcc_cb(q, t, p, _tpcc_delivery_1);
      p->oid = rand_between(0, CUSTOMERS_PER_DISTRICT);
      ncb->item = create_key(get_key_order(warehouse, i, p->oid));
      kv_trans_read(t, ncb);
   }
}

/*
 * New order query
 */
struct new_order_payload {
   long warehouse;
   long district;
   long customer;
   long oid;
   long ol_cnt;
   long item_id;
   struct items {
      long iid;
      long quantity;
      long warehouse;
   } items[];
};

static void _tpcc_new_order_7(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;
   free(p);
}

// Add order_line
static void _tpcc_new_order_6(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;

   char *new_order_line = tpcc_order_line(p->warehouse, p->district, p->oid, p->item_id);
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_7);
   ncb->item = new_order_line;
   kv_trans_write(cb->transaction, ncb);
}

// Update stock
static void _tpcc_new_order_5(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;
   if(!item)
      die("Cannot read STOCK.%ld.%ld\n", p->warehouse, p->items[p->item_id].iid);

   char *new_item = clone_item(item);
   new_item = incr_shash_uint(new_item, S_QUANTITY, p->items[p->item_id].quantity); // This is not what TPCC does, but it's the same stuff from the DB point of view

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_6);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}

static void _tpcc_new_order_4b(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;
   free(p);
}

// Get stocks of all items
static void _tpcc_new_order_4(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;

   for(size_t i = 0; i < p->ol_cnt; i++) {
      struct new_order_payload *new_payload = malloc(sizeof(*p) + p->ol_cnt * sizeof(struct items));
      memcpy(new_payload, p, sizeof(*p) + p->ol_cnt * sizeof(struct items));
      new_payload->item_id = i;

      long stock_key = get_key_stock(p->warehouse, p->items[i].iid);

      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_new_order_5);
      ncb->item = create_key(stock_key);
      kv_trans_read(cb->transaction, ncb);
   }

   free(p);
}

// Add the order in the order index of the customer
static void _tpcc_new_order_3b(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;

   void *new_item = NULL;
   if(item) {
      new_item = add_smember(item, p->oid, 0);
   } else {
      new_item = tpcc_order_index(p->warehouse, p->district, p->customer, p->oid);
   }

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_4b);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}

// Create the new order
static void _tpcc_new_order_3(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;

   char *new_order = tpcc_new_order(p->warehouse, p->district, p->oid);
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_4);
   ncb->item = new_order;
   kv_trans_write(cb->transaction, ncb);
}

// Add the order in the order table and query the order index of the customer
static void _tpcc_new_order_2(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;

   struct new_order_payload *new_payload = malloc(sizeof(*p));
   memcpy(new_payload, p, sizeof(*p));

   char *order = tpcc_order(p->warehouse, p->district, p->customer, p->oid, p->ol_cnt);
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_3);
   ncb->item = order;
   kv_trans_write(cb->transaction, ncb);

   struct slab_callback *ncb2 = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_new_order_3b);
   ncb2->item = create_key(get_key_order_index(new_payload->warehouse, new_payload->district, new_payload->customer));
   kv_trans_write(cb->transaction, ncb2);
}

// Update the last order id of the district
static void _tpcc_new_order_1(struct slab_callback *cb, void *item) {
   struct new_order_payload *p = cb->payload;
   if(!item)
      die("Couldn't find DISTRICT.%ld.%ld\n",  p->warehouse, p->district);

   p->oid = get_shash_uint(item, D_NEXT_O_ID);

   char *new_item = clone_item(item);
   incr_shash_uint(new_item, D_NEXT_O_ID, 1);

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_new_order_2);
   ncb->item = new_item;
   kv_trans_read(cb->transaction, ncb);
}

static void tpcc_new_order_query(struct injector_queue *q, struct transaction *t) {
   uint64_t cnt = rand_between(MIN_OL_CNT, MAX_OL_CNT);
   struct new_order_payload *p = malloc(sizeof(*p) + cnt * sizeof(struct items));
   p->warehouse = get_rand_warehouse();
   p->district = get_rand_district();
   p->customer = get_rand_customer();
   p->ol_cnt = cnt;
   for(size_t i = 0; i < p->ol_cnt; i++) {
again:
      p->items[i].iid = rand_between(0, NUM_ITEMS);
      for(size_t j = 0; j < i; j++)
         if(p->items[i].iid == p->items[j].iid)
            goto again;
      p->items[i].quantity = rand_between(0, MAX_QUANTITY);
      if(rand_between(0, 100) < 2)
         p->items[i].warehouse = get_rand_warehouse();
      else
         p->items[i].warehouse = p->warehouse;
   }

   struct slab_callback *ncb = tpcc_cb(q, t, p, _tpcc_new_order_1);
   ncb->item = create_key(get_key_district(p->warehouse, p->district));
   kv_trans_read(t, ncb);
}

/*
 * Payment transaction
 * 0/ Get the warehouse info
 * 1/ Update the warehouse info
 * 2/ Get the district
 * 3/ Update the district info
 * 4/ Get the customer
 * 5/ Update the customer
 * 6/ Add a history record
 * 7/ Transaction done
 */
struct payment_payload {
   long warehouse;
   long district;
   long customer;
   long h_amount;
};

static void _tpcc_payment_7(struct slab_callback *cb, void *item) {
   // Payment done
   struct payment_payload *p = cb->payload;
   free(p);
}

static void _tpcc_payment_6(struct slab_callback *cb, void *item) {
   // Update of the client has been done, now add a record in 'HISTORY'
   // Note that redisdriver.py is incorrect in that it maintains a record for the "next history ID" that is incremented for every payment
   // We don't do that to avoid write conflicts in transactions ('HISTORY' has no primary key in TPCC so that's OK)
   struct payment_payload *p = cb->payload;
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_7);
   ncb->item = tpcc_history(uniform_next());
   kv_trans_write(cb->transaction, ncb);
}

static void _tpcc_payment_5(struct slab_callback *cb, void *item) {
   // Read client info
   struct payment_payload *p = cb->payload;
   if(!item)
      die("Couldn't find CUSTOMER\n");

   enum table t = get_table_from_item(item);
   if(t == CUSTOMER_INDEXES) { // customer was searched by name, we now have its ID, so perform the actual query to get its data
      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_5);
      ncb->item = create_key(get_key_customer(p->warehouse, p->district, p->customer));
      kv_trans_read(cb->transaction, ncb);
      return;
   }

   char *new_item = NULL;
   char *is_bad = get_shash_string(item, C_CREDIT);
   if(!strcmp(is_bad, BAD_CREDIT)) {
      char *c_data = get_shash_string(item, C_DATA);
      char *new_c_data = malloc(MAX_C_DATA);
      snprintf(new_c_data, MAX_C_DATA, "%s |  %4ld  %2ld  %4ld  %2ld  %4ld  $%ld %12s %24s",
            c_data,
            get_shash_uint(item, C_ID),
            get_shash_uint(item, C_D_ID),
            get_shash_uint(item, C_W_ID),
            p->district,
            p->warehouse,
            p->h_amount,
            c_data,
            c_data
            );
      new_item = replace_shash_element(item, C_DATA, new_c_data, strlen(new_c_data) + 1, 0);
   } else {
      new_item = clone_item(item);
   }
   new_item = incr_shash_uint(new_item, C_BALANCE, p->h_amount);

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_6);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}

static void _tpcc_payment_4(struct slab_callback *cb, void *item) {
   // DISTRICT has been updated
   struct payment_payload *p = cb->payload;

   long key;
   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_5);
   if(rand_between(0, 100) < 60) { // request by customer name
      key = get_key_customer_indexes(p->warehouse, p->district, p->customer);
   } else {
      key = get_key_customer(p->warehouse, p->district, p->customer);
   }
   ncb->item = create_key(key);
   kv_trans_read(cb->transaction, ncb);
}

static void _tpcc_payment_3(struct slab_callback *cb, void *item) {
   struct payment_payload *p = cb->payload;
   if(!item)
      die("Couldn't find DISTRICT.%ld.%ld\n", p->warehouse, p->district);

   void *new_item = clone_item(item);
   incr_shash_uint(new_item, D_YTD, p->h_amount);

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_4);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}


static void _tpcc_payment_2(struct slab_callback *cb, void *item) {
   // Update of the WAREHOUSE has been done, now read the district
   struct payment_payload *p = cb->payload;

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_3);
   ncb->item = create_key(get_key_district(p->warehouse, p->district));
   kv_trans_read(cb->transaction, ncb);
}

static void _tpcc_payment_1(struct slab_callback *cb, void *item) {
   struct payment_payload *p = cb->payload;
   if(!item)
      die("Couldn't find WAREHOUSE.%ld\n", p->warehouse);

   void *new_item = clone_item(item);
   incr_shash_uint(new_item, W_YTD, p->h_amount);

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_payment_2);
   ncb->item = new_item;
   kv_trans_write(cb->transaction, ncb);
}


static void tpcc_payment_query(struct injector_queue *q, struct transaction *t) {
   struct payment_payload *p = calloc(1, sizeof(*p));
   p->warehouse = get_rand_warehouse();
   p->district = get_rand_district();
   p->customer = get_rand_customer();
   p->h_amount = rand_between(MIN_PAYMENT, MAX_PAYMENT);

   struct slab_callback *ncb = tpcc_cb(q, t, p, _tpcc_payment_1);
   ncb->item = create_key(get_key_warehouse(p->warehouse));
   kv_trans_read(t, ncb);
}

/*
 * Stock transaction = get the stock of the last 20 ordered items of a district
 * 0/ Get last order id of the district
 * 1 & 2 / for(i = last order id; i > ... - 20; i--) { get order lines of the order }
 *         This requires 2 steps: order_id -> [ order line keys ] & order line key -> order line
 * 3/   foreach(order line) { get stock of item id }
 * 4/   final cb
 */
#define NB_STOCK_ITEMS 20

struct stock_payload {
   long warehouse;
   long district;
   long oid;
   long onum;
   long iid;
};

static void _tpcc_stock_level_4(struct slab_callback *cb, void *item) {
   struct stock_payload *p = cb->payload;
   if(!item)
      die("Couldn't find STOCK.%ld.%ld\n",  p->warehouse, p->iid);
   //Count number of items which stock is low
   free(p);
}

static void _tpcc_stock_level_3(struct slab_callback *cb, void *item) {
   struct stock_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDER_LINE.%ld.%ld.%ld.%ld\n",  p->warehouse, p->district, p->oid, p->onum);

   p->iid = get_shash_uint(item, OL_I_ID);

   long stock_key = get_key_stock(p->warehouse, p->iid);

   struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, p, _tpcc_stock_level_4);
   ncb->item = create_key(stock_key);
   kv_trans_read(cb->transaction, ncb);
}

static void _tpcc_stock_level_2(struct slab_callback *cb, void *item) {
   struct stock_payload *p = cb->payload;
   if(!item)
      die("Couldn't find ORDER.%ld.%ld.%ld\n",  p->warehouse, p->district, p->oid);

   size_t nb_elements = get_shash_uint(item, O_OL_CNT);
   for(size_t i = 0; i < nb_elements; i++) {
      struct stock_payload *new_payload = malloc(sizeof(*p));
      new_payload->warehouse = p->warehouse;
      new_payload->district = p->district;
      new_payload->oid = p->oid;
      new_payload->onum = i;

      uint64_t key_order_line = get_key_order_line(p->warehouse, p->district, p->oid, i);

      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_stock_level_3);
      ncb->item = create_key(key_order_line);
      kv_trans_read(cb->transaction, ncb);
   }

   free(p);
}


static void _tpcc_stock_level_1(struct slab_callback *cb, void *item) {
   struct stock_payload *p = cb->payload;
   if(!item)
      die("Couldn't find DISTRICT.%ld.%ld\n",  p->warehouse, p->district);

   long oid = get_shash_uint(item, D_NEXT_O_ID) - 1;

   for(size_t i = 0; i < NB_STOCK_ITEMS; i++) {
      struct stock_payload *new_payload = malloc(sizeof(*p));
      new_payload->warehouse = p->warehouse;
      new_payload->district = p->district;
      new_payload->oid = oid - i;

      long order_line_key = get_key_order(p->warehouse, p->district, new_payload->oid);
      struct slab_callback *ncb = tpcc_cb(cb->injector_queue, cb->transaction, new_payload, _tpcc_stock_level_2);
      ncb->item = create_key(order_line_key);
      kv_trans_read(cb->transaction, ncb);
   }

   free(p);
}

static void tpcc_stock_level_query(struct injector_queue *q, struct transaction *t) {
   struct stock_payload *p = calloc(1, sizeof(*p));
   p->warehouse = get_rand_warehouse();
   p->district = get_rand_district();

   long district_key = get_key_district(p->warehouse, p->district);

   struct slab_callback *ncb = tpcc_cb(q, t, p, _tpcc_stock_level_1);
   ncb->item = create_key(district_key);
   kv_trans_read(t, ncb);
}

static void _launch_tpcc(struct workload *w, int test, int nb_requests, int zipfian) {
   struct injector_queue *q = create_new_injector_queue();

   //create_transaction(); // screw up the snapshots!

   declare_periodic_count;
   for(size_t i = 0; i < nb_requests; i++) {
      struct transaction *t = create_transaction();
      struct transaction_payload *p = calloc(1, sizeof(struct transaction_payload));
      set_payload(t, p);

      long x = uniform_next() % 100;
      if(x <= 4) {
         p->query_id = 0;
         tpcc_stock_level_query(q, t);
      } else if(x <= 4 + 4) {
         p->query_id = 1;
         tpcc_delivery_query(q, t);
      } else if (x <= 4 + 4 + 4) {
         p->query_id = 2;
         tpcc_orderstatus_query(q, t);
      } else if (x <= 43 + 4 + 4 + 4) {
         p->query_id = 3;
         tpcc_payment_query(q, t);
      } else {
         p->query_id = 4;
         tpcc_new_order_query(q, t);
      }

      injector_process_queue(q);
      periodic_count(1000, "TRANS Load Injector (%lu%%) [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu]", i*100LU/nb_requests, failed_trans*100/nb_commits_done, get_snapshot_size(), get_nb_running_transactions());
   }

   do {
      injector_process_queue(q);
   } while(nb_commits_done != nb_requests * w->nb_load_injectors);
}

/* Generic interface */
static void launch_tpcc(struct workload *w, bench_t b) {
   return _launch_tpcc(w, 0, w->nb_requests_per_thread, 0);
}

/* Pretty printing */
static const char *name_tpcc(bench_t w) {
   printf("TPCC BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu]\n", ((double)failed_trans)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size());
   return "TPCC";
}

static int handles_tpcc(bench_t w) {
   failed_trans = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;

   switch(w) {
      case tpcc:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_tpcc(void) {
   return "TPCC";
}

struct workload_api TPCC = {
   .handles = handles_tpcc,
   .launch = launch_tpcc,
   .api_name = api_name_tpcc,
   .name = name_tpcc,
   .create_unique_item = create_unique_item_tpcc,
};
