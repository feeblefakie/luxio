#include "btree.h"
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
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " record_num select? usermem?" << std::endl; 
    exit(1);
  }
  int mode;
  if(argc >= 3) {
    mode = atoi(argv[2]);
  }
  bool usermem = false;
  if (argc >= 4) {
    usermem = true;
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  //bt->set_lock_type(Lux::DBM::LOCK_PROCESS);
  //bt->set_page_size(4096);
  bt->open("benchdb", Lux::DB_CREAT);

  int rnum = atoi(argv[1]);
  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);
      //std::cout << "[" << key << "]" << std::endl;

      if (!bt->put(key, strlen(key), &i, sizeof(uint32_t))) {
        std::cerr << "put failed." << std::endl;
      }
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
  }

  if (mode == 2 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);
      int num = i;
      //int num = rand() % rnum;
      //sprintf(key,"%08d", num);

      if (usermem) {
        uint32_t val;
        Lux::DBM::data_t key_data = {key, strlen(key)};
        Lux::DBM::data_t val_data = {&val, sizeof(uint32_t)};

        if (bt->get(&key_data, &val_data)) { // Lux::DBM::USER omitted
          if (num != val) {
            std::cout << "[error] value incorrect." << std::endl;
          }
        } else {
          std::cout << "[error] entry not found. [" << key << "]" << std::endl;
        } 
      } else {
        Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
        if (val_data != NULL) {
          uint32_t val;
          memcpy(&val, val_data->data, val_data->size);
          if (num != val) {
            std::cout << "[error] value incorrect." << std::endl;
          }
          bt->clean_data(val_data);
        } else {
          std::cout << "[error] entry not found. [" << key << "]" << std::endl;
        } 
      }
    }
    t2 = gettimeofday_sec();
    std::cout << "get time: " << t2 - t1 << std::endl;
  }

  bt->show_db_header();

  bt->close();
  delete bt;

  return 0;
}
