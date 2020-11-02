//ifndef workload_tpcc
//define workload_tpcc

//define NB_DISTRICTS_PER_WAREHOUSE 4

#define NB_WAREHOUSES 300
/*#define NB_WAREHOUSES 1*/


/*
 * TPCC "ITEMS"
 */
#define NUM_ITEMS 10000
#define MIN_IM 1
#define MAX_IM 10000
#define MIN_PRICE 1
#define MAX_PRICE 100
#define MIN_I_NAME 14
#define MAX_I_NAME 24
#define MIN_I_DATA 26
#define MAX_I_DATA 50

//  Warehouse constants
#define MIN_TAX 0
#define MAX_TAX 0.2000
#define TAX_DECIMALS 4
#define INITIAL_W_YTD 300000.00
#define MIN_NAME 6
#define MAX_NAME 10
#define MIN_STREET 10
#define MAX_STREET 20
#define MIN_CITY 10
#define MAX_CITY 20
#define STATE 2
#define ZIP_LENGTH 9
#define ZIP_SUFFIX "11111"

//  Stock constants
#define MIN_QUANTITY 10
#define MAX_QUANTITY 100
#define DIST 24
#define STOCK_PER_WAREHOUSE 100000

//  District constants
#define DISTRICTS_PER_WAREHOUSE 10
#define INITIAL_D_YTD 30000.00  //  different from Warehouse
#define INITIAL_NEXT_O_ID 3001

//  Customer constants
#define CUSTOMERS_PER_DISTRICT 3000
#define INITIAL_CREDIT_LIM 50000.00
#define MIN_DISCOUNT 0.0000
#define MAX_DISCOUNT 0.5000
#define DISCOUNT_DECIMALS 4
#define INITIAL_BALANCE -10.00
#define INITIAL_YTD_PAYMENT 10.00
#define INITIAL_PAYMENT_CNT 1
#define INITIAL_DELIVERY_CNT 0
#define MIN_FIRST 6
#define MAX_FIRST 10
#define MIDDLE "OE"
#define PHONE 16
#define MIN_C_DATA 300
#define MAX_C_DATA 500
#define GOOD_CREDIT "GC"
#define BAD_CREDIT "BC"

//  Order constants
#define MIN_CARRIER_ID 1
#define MAX_CARRIER_ID 10
//  HACK: This is not strictly correct, but it works
#define NULL_CARRIER_ID 0L
//  o_id < than this value, carrier != null, >= -> carrier == null
#define NULL_CARRIER_LOWER_BOUND 2101
#define MIN_OL_CNT 5
#define MAX_OL_CNT 15
#define AVG_OL_CNT 10
#define INITIAL_ALL_LOCAL 1
#define INITIAL_ORDERS_PER_DISTRICT 3000

//  Used to generate new order transactions
#define MAX_OL_QUANTITY 10

//  Order line constants
#define INITIAL_QUANTITY 5
#define MIN_AMOUNT 0.01

//  History constants
#define MIN_DATA 12
#define MAX_DATA 24
#define INITIAL_AMOUNT 10.00

//  New order constants
#define INITIAL_NEW_ORDERS_PER_DISTRICT 900

//  TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
#define INVALID_ITEM_MESSAGE "Item number is not valid"

//  Used to generate stock level transactions
#define MIN_STOCK_LEVEL_THRESHOLD 10
#define MAX_STOCK_LEVEL_THRESHOLD 20

//  Used to generate payment transactions
#define MIN_PAYMENT 1.0
#define MAX_PAYMENT 5000.0


//endif
