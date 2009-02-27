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
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " file dbname usermem(1/0)" << std::endl; 
    exit(1);
  }

  double t1, t2;
  FILE *fp = fopen(argv[1], "r");
  if (fp == NULL) {
    std::cerr << "failed to open " << argv[1] << std::endl;
    exit(1);
  }

  bool usermem = false;
  if (atoi(argv[3]) == 1) {
    usermem = true;
  }

  DB *dbp;
  u_int32_t flags;
  int ret;

  t1 = gettimeofday_sec();
  ret = db_create(&dbp, NULL, 0);
  if (ret != 0) {
    fprintf(stderr, "create failed\n");
    exit(1);
  }

  flags = DB_RDONLY;
  dbp->set_cachesize(dbp, 0, 512*1024*1024, 0);
  ret = dbp->open(dbp, NULL, argv[2], NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    /* Error handling goes here */
    fprintf(stderr, "open failed\n");
    exit(1);
  }

  char buf[256];
  while (fgets(buf, 256, fp)) {
    char *str = strchr(buf, ' ');
    if (str == NULL) { continue; }
    *str++ = '\0';
    char *valp = strchr(str, ' ');
    if (valp == NULL) { continue; }
    *valp++ = '\0';
    int32_t aval = atoi(valp);

    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = str;
    key.size = strlen(str);

    if (usermem) {
      int32_t val;
      data.data = &val;
      data.ulen = sizeof(int32_t);
      data.flags = DB_DBT_USERMEM;
      int ret = dbp->get(dbp, NULL, &key, &data, 0);

      if (ret != DB_NOTFOUND) {
        if (val != aval) {
          std::cout << "[error] value incorrect." << std::endl;
        }
      } else {
        std::cout << "[error] entry not found." << std::endl;
      } 
    } else {
      data.flags = DB_DBT_MALLOC;
      ret = dbp->get(dbp, NULL, &key, &data, 0);

      if (ret != DB_NOTFOUND) {
        if (*(int32_t *) data.data != aval) {
          std::cout << "[error] value incorrect." << std::endl;
        }
      } else {
        std::cout << "[error] entry not found." << std::endl;
      } 
    }
  }
  dbp->close(dbp, 0); 
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  return 0;
}
