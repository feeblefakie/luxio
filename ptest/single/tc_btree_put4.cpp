#include <tcutil.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
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
int main(int argc, char **argv){

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname record_num" << std::endl; 
    exit(1);
  }

  double t1, t2;
  int rnum = atoi(argv[2]);

  int ecode;
  char *key, *value;

  t1 = gettimeofday_sec();
  TCBDB *bdb = tcbdbnew();
  tcbdbtune(bdb, 5, -1, -1, -1, -1, 0);
  tcbdbsetcache(bdb, 300, 100);
  if (!tcbdbopen(bdb, argv[1], BDBOWRITER | BDBOCREAT)) {
    ecode = tcbdbecode(bdb);
    fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
  }

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    char *val = new char[102401]; // 100K
    sprintf(val, "%0102400d", i);

    if (!tcbdbput(bdb, key, strlen(key), val, strlen(val))) {
      fprintf(stderr, "put error\n");
    }
    delete [] val;
  }

  tcbdbsync(bdb);
  if (!tcbdbclose(bdb)) {
    ecode = tcbdbecode(bdb);
    fprintf(stderr, "close error: %s\n", tcbdberrmsg(ecode));
  }
  tcbdbdel(bdb);
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  return 0;
}
