#include <db.h> 
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>
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

DB *dbp;
int num_recs = 0;
int num_updated = 0;
typedef void *(*PROC)(void *);

int main(int argc, char **argv){

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " record_num" << std::endl; 
    exit(1);
  }

  u_int32_t flags;
  int ret;

  ret = db_create(&dbp, NULL, 0);
  if (ret != 0) {
    fprintf(stderr, "create failed\n");
    exit(1);
  }

  flags = DB_CREATE | DB_THREAD;

  dbp->set_cachesize(dbp, 0, 512*1024*1024, 0);
  ret = dbp->open(dbp, NULL, "my_db.db", NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    /* Error handling goes here */
  }

  int rnum = atoi(argv[1]);

  write_seq(&rnum);

  pthread_t thread_writer, thread_reader;
  double t1, t2;

  t1 = gettimeofday_sec();
  pthread_create(&thread_writer, NULL, (PROC)update_random, (void *) &rnum);
  pthread_create(&thread_reader, NULL, (PROC)read_ramdom, (void *) &rnum);
  pthread_join(thread_writer, NULL);
  pthread_join(thread_reader, NULL);
  t2 = gettimeofday_sec();
  std::cout << "time(threads): " << t2 - t1 << std::endl;

  if (dbp != NULL) {
    dbp->close(dbp, 0); 
  }
}

void write_seq(int *rnum)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *rnum; ++i) {
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

    key.data = str;
    key.size = strlen(str);
    data.data = &i;
    data.size = sizeof(int);

    ret = dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
    if (ret == DB_KEYEXIST) {
      //fprintf(stderr, "duplicate\n");
    }
    ++num_recs;
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
}

void update_random(int *rnum)
{
  double t1, t2;
  int update_num = *rnum / 10;
  t1 = gettimeofday_sec();
  for (int i = 0; i < update_num; ++i) {
    DBT key, data;
    int ret;
    char str[9];
    memset(str, 0, 9);
    int num = rand() % *rnum;
    sprintf(str,"%08d", num);

    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = str;
    key.size = strlen(str);
    data.data = &num;
    data.size = sizeof(int);

    ret = dbp->put(dbp, NULL, &key, &data, 0);
    if (ret == DB_KEYEXIST) {
      //fprintf(stderr, "duplicate\n");
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
    char str[9];
    memset(str, 0, 9);
    if (num_recs == 0) { continue; }
    int rec_num = (int) random() % num_recs;
    sprintf(str,"%08d", rec_num);

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
      if (rec_num != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
    } else {
      std::cout << "[error] entry not found for " << str << std::endl;
    } 
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
}
