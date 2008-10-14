#include "array.h"
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

void update_random(int *num);
void write_seq(int *num);
void read_ramdom(int *num);

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

int rnum;
int num_recs = 0;
int num_updated = 0;
int num_writes = 0;
int num_reads = 0;
typedef void *(*PROC)(void *);
Lux::DBM::Array *ary;

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " 
              << argv[0] 
              << " num_record num_write_threads num_read_threads" 
              << std::endl; 
    exit(1);
  }

  ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
  ary->set_lock_type(Lux::DBM::LOCK_THREAD);
  ary->open("arraydb", Lux::DB_CREAT);

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

  ary->close();
  delete ary;

  return 0;
}

void write_seq(int *num)
{
  std::cout << "*num: " << *num << std::endl;
  double t1, t2;
  int from = rnum / num_writes * (*num);
  int to = rnum / num_writes * (*num + 1);
  std::cout << "from: " << from << ", to: " << to << std::endl;
  t1 = gettimeofday_sec();
  for (int i = from; i < to; ++i) {
    ary->put(i, &i, sizeof(uint32_t));
    ++num_recs;
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
}

void read_ramdom(int *num)
{
  double t1, t2;
  if (num_recs == 0) { num_recs = rnum; }
  int from = rnum / num_reads * (*num);
  int to = rnum / num_reads * (*num + 1);
  t1 = gettimeofday_sec();
  for (int i = from; i < to; ++i) {
    Lux::DBM::data_t data;
    uint32_t val, size;
    data.data = &val;
    data.size = sizeof(uint32_t);
    ary->get(i, &data, &size);
    if (val != i) {
      std::cout << "[error] value incorrect. i=" << i << ", data=" << val << std::endl;
    }
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
}
