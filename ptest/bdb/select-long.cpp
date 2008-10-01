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
    std::cerr << argv[0] << " key" << std::endl;
    exit(-1);
  }

  DB *dbp;
  u_int32_t flags;
  int ret;

  ret = db_create(&dbp, NULL, 0);
  if (ret != 0) {
    fprintf(stderr, "create failed\n");
    exit(1);
  }

  flags = DB_RDONLY;

  dbp->set_cachesize(dbp, 0, 512*1024*1024, 0);
  dbp->set_pagesize(dbp, 65536);
  ret = dbp->open(dbp, NULL, "my_db.db", NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    /* Error handling goes here */
    std::cerr << "open failed" << std::endl;
    exit(-1);
  }

  double t1, t2;
  t1 = gettimeofday_sec();

  DBT key, data;
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = argv[1];
  key.size = strlen(argv[1]);

  data.flags = DB_DBT_MALLOC;
  ret = dbp->get(dbp, NULL, &key, &data, 0);

  if (ret != DB_NOTFOUND) {
    std::cout << "HIT!" << std::endl;
    std::cout << data.size << std::endl;
    std::cout.write((char *) data.data, data.size);
    std::cout << std::endl;
  } else {
    std::cout << "[error] entry not found." << std::endl;
  } 
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  if (dbp != NULL) {
    dbp->close(dbp, 0); 
  }
}
