#include <tcutil.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

void update_random(int *rnum);
void write_seq(int *rnum);
void read_ramdom(int *rnum);

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

TCBDB *bdb;
int num_recs = 0;
int num_updated = 0;
typedef void *(*PROC)(void *);

int main(int argc, char **argv){

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " record_num" << std::endl; 
    exit(1);
  }

  int ecode;
  char *key, *value;

  /* オブジェクトを作成する */
  bdb = tcbdbnew();
  if(!tcbdbsetmutex(bdb)){
    fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
    exit(-1);
  }

  tcbdbtune(bdb, 240, -1, -1, -1, -1, 0);
  //tcbdbtune(bdb, 480, -1, -1, -1, -1, BDBTDEFLATE);
  tcbdbsetcache(bdb, 20480, 10240);
  //tcbdbsetxmsiz(bdb, 134217728);

  /* データベースを開く */
  if(!tcbdbopen(bdb, "casket.bdb", BDBOWRITER | BDBOCREAT)){
  ecode = tcbdbecode(bdb);
    fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
  }

  int rnum = atoi(argv[1]);
  write_seq(&rnum);

  double t1, t2;
  pthread_t thread_writer, thread_reader;

  t1 = gettimeofday_sec();
  pthread_create(&thread_writer, NULL, (PROC)update_random, (void *) &rnum);
  pthread_create(&thread_reader, NULL, (PROC)read_ramdom, (void *) &rnum);
  pthread_join(thread_writer, NULL);
  pthread_join(thread_reader, NULL);
  t2 = gettimeofday_sec();
  std::cout << "time(threads): " << t2 - t1 << std::endl;

  /* データベースを閉じる */
  if(!tcbdbclose(bdb)){
  ecode = tcbdbecode(bdb);
  fprintf(stderr, "close error: %s\n", tcbdberrmsg(ecode));
  }

  /* オブジェクトを破棄する */
  tcbdbdel(bdb);

  return 0;
}

void write_seq(int *rnum)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *rnum; ++i) {
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
    ++num_recs;
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
}

void update_random(int *rnum)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *rnum/10; ++i) {
    char key[9];
    memset(key, 0, 9);
    int num = rand() % *rnum;
    sprintf(key,"%08d", num);
    //std::cout << "[" << key << "]" << std::endl;
    /* レコードを格納する */
    //printf("%d\n", i);
    if(!tcbdbput(bdb, key, strlen(key), &num, sizeof(int))) {
      fprintf(stderr, "put error\n");
    }
    ++num_updated;
    usleep(15);
  }
  t2 = gettimeofday_sec();
  std::cout << "update time: " << t2 - t1 << std::endl;
  std::cout << "num_updated: " << num_updated << std::endl;
}

void read_ramdom(int *rnum)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    //sprintf(key,"%08d", i);
    if (num_recs == 0) { continue; }
    int rec_num = (int) random() % num_recs;
    sprintf(key,"%08d", rec_num);

    int size;
    void *data = tcbdbget(bdb, key, strlen(key), &size);
    if (data != NULL) {
      uint32_t val;
      memcpy(&val, data, size);
      if (rec_num != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
      free(data);
    } else {
      std::cout << "[error] entry not found." << std::endl;
    } 
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
}
