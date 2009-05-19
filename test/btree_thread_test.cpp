#include "../btree.h"
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
int num_selects = 0;
int num_reads = 0;
bool write_finished = false;
typedef void *(*PROC)(void *);
Lux::IO::Btree *bt;

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " 
              << argv[0] 
              << " num_record num_read_threads" 
              << std::endl; 
    exit(1);
  }

  bt = new Lux::IO::Btree(Lux::IO::CLUSTER);
  bt->set_lock_type(Lux::IO::LOCK_THREAD);
  if (!bt->open("bttt", Lux::IO::DB_CREAT)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }
  
  rnum = atoi(argv[1]);
  num_reads = atoi(argv[2]);

  pthread_t *writer = new pthread_t;
  pthread_t *readers = new pthread_t[num_reads];
  int rarr[128], warr = 1;
  double t1, t2;

  for (int i = 0; i < num_reads; ++i) {
    rarr[i] = i;
  }

  t1 = gettimeofday_sec();
  if (pthread_create(writer, NULL, (PROC)write_seq, (void *) &rnum) != 0) {
    std::cerr << "pthread_create failed" << std::endl;
  }
  for (int i = 0; i < num_reads; ++i) {
    if (pthread_create(&readers[i], NULL, (PROC)read_ramdom, (void *) &rarr[i]) != 0) {
      std::cerr << "pthread_create failed" << std::endl;
    }
  }
  pthread_join(*writer, NULL);
  for (int i = 0; i < num_reads; ++i) {
    pthread_join(readers[i], NULL);
  }
  t2 = gettimeofday_sec();
  std::cout << "time(threads): " << t2 - t1 << std::endl;

  std::cout << "num selects: " << num_selects << std::endl;

  bt->close();
  delete bt;

  return 0;
}

void write_seq(int *num)
{
  std::cout << "*num: " << (int) *num << std::endl;
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < *num; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);

    bt->put(key, strlen(key), &i, sizeof(uint32_t));
    ++num_recs;
    if (num_recs % 100 == 0) {
      std::cout << "num_recs: " << num_recs << std::endl;
      usleep(1);
    }
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
  write_finished = true;
}

void read_ramdom(int *num)
{
  double t1, t2;
  while (!write_finished) {
    char key[9];
    memset(key, 0, 9);
    if (num_recs == 0) {
      usleep(1);
      continue;
    }
    int rec_num = (int) (random() % num_recs);
    sprintf(key,"%08d", rec_num);

    Lux::IO::data_t *val_data = bt->get(key, strlen(key));
    if (val_data != NULL) {
      if (rec_num != *(uint32_t *) val_data->data) {
        std::cout << "[error] value incorrect." << std::endl;
      }
      bt->clean_data(val_data);
    } else {
      std::cout << "[error] entry not found. [" << key << "]" << std::endl;
    } 
    ++num_selects;
    if (num_selects % 100 == 0) {
      usleep(1);
    }
  }
}
