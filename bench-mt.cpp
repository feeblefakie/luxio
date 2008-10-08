#include "btree.h"
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
typedef void *(*PROC)(void *);
Lux::DBM::Btree *bt;

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " record_num thread_num" << std::endl; 
    exit(1);
  }

  bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  bt->open("benchdb", Lux::DB_CREAT);

  rnum = atoi(argv[1]);
  int tnum = atoi(argv[2]);

  write_seq(&rnum);

  pthread_attr_t sched_attr;
  pthread_attr_init(&sched_attr);
  //pthread_attr_setschedpolicy(&sched_attr, SCHED_RR);
  pthread_attr_setscope(&sched_attr, PTHREAD_SCOPE_SYSTEM);
  pthread_t tw, *thread;
  thread = new pthread_t[tnum];
  double t1, t2;

  t1 = gettimeofday_sec();
  //pthread_create(&tw, NULL, (PROC)update_random, (void *) &rnum);
  for (int i = 0; i < tnum; ++i) {
    //if (pthread_create(&thread[i], &sched_attr, (PROC)read_ramdom, (void *) &i) != 0) {
    if (pthread_create(&thread[i], NULL, (PROC)read_ramdom, (void *) &i) != 0) {
      std::cerr << "pthread_create failed" << std::endl;
    }
    //pthread_join(thread[i], NULL);
  }
  //pthread_join(tw, NULL);
  ///*
  for (int i = 0; i < tnum; ++i) {
    pthread_join(thread[i], NULL);
  }
  //*/
  t2 = gettimeofday_sec();
  std::cout << "time(threads): " << t2 - t1 << std::endl;

  bt->close();
  delete bt;

  return 0;
}

void write_seq(int *num)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    //std::cout << "[" << key << "]" << std::endl;

    bt->put(key, strlen(key), &i, sizeof(uint32_t));
    ++num_recs;
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
}

void update_random(int *num)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum/10; ++i) {
    char key[9];
    memset(key, 0, 9);
    int rec_num = (int) random() % rnum;
    sprintf(key,"%08d", rec_num);
    //std::cout << "[" << key << "]" << std::endl;

    bt->put(key, strlen(key), &rec_num, sizeof(uint32_t));
    ++num_updated;
    usleep(15);
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;
  std::cout << "num_updated: " << num_updated << std::endl;
}

void read_ramdom(int *num)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    //sprintf(key,"%08d", i);
    //int num = i;
    if (num_recs == 0) { continue; }
    int rec_num = (int) random() % num_recs;
    sprintf(key,"%08d", rec_num);
    //std::cout << "[" << *num << "] " << key << std::endl;

    Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
    if (val_data != NULL) {
      uint32_t val;
      memcpy(&val, val_data->data, val_data->size);
      if (rec_num != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
      bt->clean_data(val_data);
    } else {
      std::cout << "[error] entry not found. [" << key << "]" << std::endl;
    } 
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;
}
