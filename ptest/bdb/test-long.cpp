#include <db.h> 
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>

#define VALSIZE 160000

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}
int main(int argc, char **argv){

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " record_num" << std::endl; 
    exit(1);
  }

  DB *dbp;
  u_int32_t flags;
  int ret;

  ret = db_create(&dbp, NULL, 0);
  if (ret != 0) {
    fprintf(stderr, "create failed\n");
    exit(1);
  }

  flags = DB_CREATE;

  dbp->set_cachesize(dbp, 0, 512*1024*1024, 0);
  dbp->set_pagesize(dbp, 65536);
  ret = dbp->open(dbp, NULL, "my_db.db", NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    /* Error handling goes here */
  }

  double t1, t2;
  int rnum = atoi(argv[1]);

  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    DBT key, data;
    int ret;
    char str[9];
    memset(str, 0, 9);
    //int num = rand() % rnum;
    //sprintf(str,"%08d", num);
    sprintf(str,"%08d", i);

    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    char val[VALSIZE+1];
    memset(val, 0, VALSIZE+1);
    sprintf(val, "%0160000d", i);

    key.data = str;
    key.size = strlen(str);
    data.data = val;
    data.size = VALSIZE;

    ret = dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
    if (ret == DB_KEYEXIST) {
      //fprintf(stderr, "duplicate\n");
    }
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

/*
  int select_num = rnum / 10;
  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    char str[9];
    memset(str, 0, 9);
    //sprintf(key,"%08d", i);
    int num = rand() % rnum;
    sprintf(str,"%08d", num);

    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = str;
    key.size = strlen(str);

    uint32_t val;
    data.data = &val;
    data.ulen = sizeof(uint32_t);
    data.flags = DB_DBT_USERMEM;
    int ret = dbp->get(dbp, NULL, &key, &data, 0);

    if (ret != DB_NOTFOUND) {
      uint32_t val;
      memcpy(&val, data.data, data.ulen);
      if (num != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
    } else {
      std::cout << "[error] entry not found." << std::endl;
    } 
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
  */

  if (dbp != NULL) {
    dbp->close(dbp, 0); 
  }
}
