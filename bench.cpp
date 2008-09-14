#include <iostream>
#include "btree.h"
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
    std::cerr << "Usage: " << argv[0] << " record_num select?" << std::endl; 
    exit(1);
  }
  int mode;
  if(argc == 3) {
    mode = atoi(argv[2]);
  }

  LibMap::Btree *bt = new LibMap::Btree(LibMap::CLUSTER);
  bt->open("benchdb", LibMap::DB_CREAT);

  int rnum = atoi(argv[1]);
  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);
      //std::cout << "[" << key << "]" << std::endl;

      bt->put(key, strlen(key), &i, sizeof(uint32_t));
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
   
    bt->show_db_header();
    if (argc != 3) {
      return 0;
    }
  }

  if (mode == 2 || mode == 3) {

    int select_num = rnum / 10;
    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      //sprintf(key,"%08d", i);
      //int num = i;
      int num = rand() % rnum;
      sprintf(key,"%08d", num);

      LibMap::data_t *val_data = bt->get(key, strlen(key));
      if (val_data != NULL) {
        uint32_t val;
        memcpy(&val, val_data->data, val_data->size);
        if (num != val) {
          std::cout << "[error] value incorrect." << std::endl;
        }
        bt->clean_data(val_data);
      } else {
        std::cout << "[error] entry not found." << std::endl;
      } 
    }
    t2 = gettimeofday_sec();
    std::cout << "get time: " << t2 - t1 << std::endl;
  }

  bt->close();

  return 0;
}
