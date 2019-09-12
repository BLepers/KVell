#include "headers.h"
#include "random.h"
#include <math.h>

/* xorshf96 */
static unsigned long x=123456789, y=362436069, z=521288629;
unsigned long xorshf96(void) {          //period 2^96-1
   unsigned long t;
   x ^= x << 16;
   x ^= x >> 5;
   x ^= x << 1;

   t = x;
   x = y;
   y = z;
   z = t ^ x ^ y;

   return z;
}

/* Thread safe xorshf96 */
static __thread unsigned long _x=123456789, _y=362436069, _z=521288629;
unsigned long locxorshf96(void) {          //period 2^96-1
   unsigned long t;
   _x ^= _x << 16;
   _x ^= _x >> 5;
   _x ^= _x << 1;

   t = _x;
   _x = _y;
   _y = _z;
   _z = t ^ _x ^ _y;

   return _z;
}

/* Init a per thread seed in a thread safe way */
static unsigned int __thread seed;
void init_seed(void) {
   seed = rand();
}

/* zipf - from https://bitbucket.org/theoanab/rocksdb-ycsb/src/master/util/zipf.h */
static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function

void init_zipf_generator(long min, long max);
double zeta(long st, long n, double initialsum);
double zetastatic(long st, long n, double initialsum);
long next_long(long itemcount);
long zipf_next();
void set_last_value(long val);

void init_zipf_generator(long min, long max){
	items = max-min+1;
	base = min;
	zipfianconstant = 0.99;
	theta = zipfianconstant;
	zeta2theta = zeta(0, 2, 0);
	alpha = 1.0/(1.0-theta);
	zetan = zetastatic(0, max-min+1, 0);
	countforzeta = items;
	eta=(1 - pow(2.0/items,1-theta) )/(1-zeta2theta/zetan);

	zipf_next();
}


double zeta(long st, long n, double initialsum) {
	countforzeta=n;
	return zetastatic(st,n,initialsum);
}

//initialsum is the value of zeta we are computing incrementally from
double zetastatic(long st, long n, double initialsum){
	double sum=initialsum;
	for (long i=st; i<n; i++){
		sum+=1/(pow(i+1,theta));
	}
	return sum;
}

long next_long(long itemcount){
	//from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
	if (itemcount!=countforzeta){
		if (itemcount>countforzeta){
			printf("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount= %ld; countforzeta= %ld)", itemcount, countforzeta);
			//we have added more items. can compute zetan incrementally, which is cheaper
			zetan = zeta(countforzeta,itemcount,zetan);
			eta = ( 1 - pow(2.0/items,1-theta) ) / (1-zeta2theta/zetan);
		}
	}

	double u = (double)(rand_r(&seed)%RAND_MAX) / ((double)RAND_MAX);
	double uz=u*zetan;
	if (uz < 1.0){
		return base;
	}

	if (uz<1.0 + pow(0.5,theta)) {
		return base + 1;
	}
	long ret = base + (long)((itemcount) * pow(eta*u - eta + 1, alpha));
	return ret;
}

long zipf_next() {
	return next_long(items);
}

/* Uniform */
long uniform_next() {
   return rand_r(&seed) % items;
}

/* bogus rand */
long bogus_rand() {
   return rand_r(&seed) % 1000;
}

/* production workload randomness */
long production_random1(void) {
   long rand_key = rand_r(&seed);
   long prob = rand_r(&seed) % 10000;
   if (prob <13) {
      rand_key = 0 + rand_key % 144000000;
   } else if (prob < 8130) {
      rand_key = 144000000 + rand_key % (314400000-144000000);
   } else if (prob < 9444) {
      rand_key = 314400000 + rand_key % (450000000-314400000);
   } else if (prob < 9742) {
      rand_key = 450000000 + rand_key % (480000000-450000000);
   } else if (prob < 9920) {
      rand_key = 480000000 + rand_key % (490000000-480000000);
   } else {
      rand_key = 490000000 + rand_key % (500000000-490000000);
   }
   return rand_key;
}

long production_random2(void) {
   long rand_key = rand_r(&seed);
   long prob = rand_r(&seed) % 10000;
   if (prob < 103487) {
      rand_key = rand_key % 47016400;
   } else if (prob < 570480) {
      rand_key = 47016400 + rand_key % (259179450 - 47016400);
   } else if (prob < 849982) {
      rand_key = 259179450 + rand_key % (386162550 - 259179450);
   } else if (prob < 930511) {
      rand_key = 386162550 + rand_key % (422748200 - 386162550);
   } else if (prob < 973234) {
      rand_key = 422748200 + rand_key % (442158000 - 422748200);
   } else if (prob < 986958) {
      rand_key = 442158000 + rand_key % (448392900 - 442158000) ;
   } else {
      rand_key = 448392900 + rand_key % (500000000 - 448392900);
   }
   return rand_key;
}

/* Helper */
const char *get_function_name(random_gen_t f) {
   if(f == zipf_next)
      return "Zipf";
   if(f == uniform_next)
      return "Uniform";
   if(f == bogus_rand)
      return "Cached";
   if(f == production_random1)
      return "Production1";
   if(f == production_random2)
      return "Production2";
   return "Unknown random";
}






