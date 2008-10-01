#include <tcutil.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

#define VALSIZE 160000

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}
int main(int argc, char **argv){

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " key" << std::endl; 
    exit(1);
  }

  TCBDB *bdb;
  BDBCUR *cur;
  int ecode;
  char *key, *value;

  /* オブジェクトを作成する */
  bdb = tcbdbnew();

  tcbdbtune(bdb, 240, -1, -1, -1, -1, 0);
  //tcbdbtune(bdb, 480, -1, -1, -1, -1, BDBTDEFLATE);
  tcbdbsetcache(bdb, 20480, 10240);
  //tcbdbsetxmsiz(bdb, 134217728);

  /* データベースを開く */
  if(!tcbdbopen(bdb, "casket.bdb", BDBOREADER)){
  ecode = tcbdbecode(bdb);
  fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
  }

  double t1, t2;

  t1 = gettimeofday_sec();
  int size;
  void *data = tcbdbget(bdb, argv[1], strlen(argv[1]), &size);
  if (data != NULL) {
    std::cout.write((char *) data, size);
    std::cout << std::endl;
    free(data);
  } else {
    std::cout << "[error] entry not found." << std::endl;
  } 
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  /* データベースを閉じる */
  if(!tcbdbclose(bdb)){
  ecode = tcbdbecode(bdb);
  fprintf(stderr, "close error: %s\n", tcbdberrmsg(ecode));
  }

  /* オブジェクトを破棄する */
  tcbdbdel(bdb);

  return 0;
}

