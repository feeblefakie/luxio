#include <db.h>
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname record_num" << std::endl; 
    exit(1);
  }
  int rnum = atoi(argv[2]);

  double t1, t2;
  t1 = gettimeofday_sec();
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
  //dbp->set_pagesize(dbp, 65536);
  ret = dbp->open(dbp, NULL, argv[1], NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    /* Error handling goes here */
    fprintf(stderr, "open failed\n");
    exit(1);
  }

  for (int i = 0; i < rnum; ++i) {
    char str[9];
    memset(str, 0, 9);
    sprintf(str,"%08d", i);
    char *val = new char[307201]; // 100K
    sprintf(val, "%0307200d", i);
    sprintf(val + 102400, "%0102400d", i);
    sprintf(val + 102400*2, "%0102400d", i);

    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = str;
    key.size = strlen(str);

    data.flags = DB_DBT_MALLOC;
    ret = dbp->get(dbp, NULL, &key, &data, 0);

    if (ret != DB_NOTFOUND) {
      if (strcmp(val, (char *) data.data) != 0) {
        std::cout << "[error] value incorrect." << std::endl;
      }
    } else {
      std::cout << "[error] entry not found." << std::endl;
    } 
    delete [] val;
  }
  dbp->close(dbp, 0); 
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  return 0;
}
