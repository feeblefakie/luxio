#include <db.h> 
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <unistd.h>

void update_random(int *num);
void write_seq(int *num);
void read_ramdom(int *num);

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

DB *dbp;
int rnum;
int num_recs = 0;
int num_updated = 0;
int num_writes = 0;
int num_reads = 0;
typedef void *(*PROC)(void *);

int main(int argc, char **argv)
{
  if (argc != 4) {
    std::cerr << "Usage: " 
              << argv[0] 
              << " num_record num_write_threads num_read_threads" 
              << std::endl; 
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

  rnum = atoi(argv[1]);
  num_writes = atoi(argv[2]);
  num_reads = atoi(argv[3]);

  pthread_t *writers = new pthread_t[num_writes];
  pthread_t *readers = new pthread_t[num_reads];
  int rarr[128], warr[128];
  double t1, t2;

  for (int i = 0; i < num_writes; ++i) {
    warr[i] = i;
  }
  for (int i = 0; i < num_reads; ++i) {
    rarr[i] = i;
  }

  t1 = gettimeofday_sec();
  for (int i = 0; i < num_writes; ++i) {
    if (pthread_create(&writers[i], NULL, (PROC)write_seq, (void *) &warr[i]) != 0) {
      std::cerr << "pthread_create failed" << std::endl;
    }
  }
  for (int i = 0; i < num_reads; ++i) {
    if (pthread_create(&readers[i], NULL, (PROC)read_ramdom, (void *) &rarr[i]) != 0) {
      std::cerr << "pthread_create failed" << std::endl;
    }
  }
  for (int i = 0; i < num_writes; ++i) {
    pthread_join(writers[i], NULL);
  }
  for (int i = 0; i < num_reads; ++i) {
    pthread_join(readers[i], NULL);
  }
  t2 = gettimeofday_sec();
  std::cout << "time(threads): " << t2 - t1 << std::endl;

  if (dbp != NULL) {
    dbp->close(dbp, 0); 
  }
}

void write_seq(int *num)
{
  double t1, t2;
  int from = rnum / num_writes * (*num);
  int to = rnum / num_writes * (*num + 1);
  t1 = gettimeofday_sec();
  for (int i = from; i < to; ++i) {
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

void update_random(int *num)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *num/10; ++i) {
    DBT key, data;
    int ret;
    char str[9];
    memset(str, 0, 9);
    int n = rand() % (*num);
    sprintf(str,"%08d", n);

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

void read_ramdom(int *num)
{
  double t1, t2; 
  int from = rnum / num_reads * (*num);
  int to = rnum / num_reads * (*num + 1);
  t1 = gettimeofday_sec();
  for (int i = from; i < to; ++i) {
    char str[9];
    memset(str, 0, 9);
    sprintf(str,"%08d", i);
    int rec_num = i;
    /*
    if (num_recs == 0) { continue; }
    int rec_num = (int) random() % num_recs;
    sprintf(str,"%08d", rec_num);
    */

    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = str;
    key.size = strlen(str);
    //uint32_t val;
    //data.data = &val;
    data.ulen = sizeof(uint32_t);
    //data.flags = DB_DBT_USERMEM;
    data.flags = DB_DBT_MALLOC;
    int ret = dbp->get(dbp, NULL, &key, &data, 0);

    if (ret != DB_NOTFOUND) {
      uint32_t val;
      memcpy(&val, data.data, data.ulen);
      if (rec_num != val) {
        std::cout << "[error] value incorrect. " << rec_num << ", " << val << std::endl;
      }
    } else {
      std::cout << "[error] entry not found for " << str << std::endl;
    } 
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
}
