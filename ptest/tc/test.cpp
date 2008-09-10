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

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " record_num" << std::endl; 
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
  if(!tcbdbopen(bdb, "casket.bdb", BDBOWRITER | BDBOCREAT)){
  ecode = tcbdbecode(bdb);
  fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
  }

  double t1, t2;
  int rnum = atoi(argv[1]);

  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    //int num = rand() % rnum;
    //sprintf(key,"%08d", num);
    sprintf(key,"%08d", i);
    //std::cout << "[" << key << "]" << std::endl;
    /* レコードを格納する */
    //printf("%d\n", i);
    if(!tcbdbput(bdb, key, strlen(key), &i, sizeof(int))) {
      fprintf(stderr, "put error\n");
    }
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
  
  int select_num = rnum / 10;
  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    //sprintf(key,"%08d", i);
    int num = rand() % rnum;
    sprintf(key,"%08d", num);

    int size;
    void *data = tcbdbget(bdb, key, strlen(key), &size);
    if (data != NULL) {
      uint32_t val;
      memcpy(&val, data, size);
      if (num != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
      free(data);
    } else {
      std::cout << "[error] entry not found." << std::endl;
    } 
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

