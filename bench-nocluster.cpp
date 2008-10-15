#include "btree.h"
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

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  if (!bt->open("benchdb", Lux::DB_CREAT)) {
    std::cerr << "opening database failed." << std::endl;
    exit(-1);
  }

  int rnum = atoi(argv[1]);
  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);
      //std::cout << "[" << key << "]" << std::endl;
      char val[VALSIZE+1];
      memset(val, 0, VALSIZE+1);
      sprintf(val, "%0160000d", i);
      //std::cout << "[" << val << "]" << std::endl;

      bt->put(key, strlen(key), val, VALSIZE);
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
   
    if (argc != 3) {
      return 0;
    }
  }

  if (mode == 2 || mode == 3) {

    char val2[VALSIZE+1];
    memset(val2, 0, VALSIZE+1);

    int select_num = rnum / 10;
    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);
      int num = i;
      //int num = rand() % rnum;
      //sprintf(key,"%08d", num);
      char val[VALSIZE+1];
      memset(val, 0, VALSIZE+1);
      sprintf(val, "%016000d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      if (val_data != NULL) {
        memcpy(&val2, val_data->data, val_data->size);
        if (strcmp(val, val2) != 0) {
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

  bt->show_db_header();

  bt->close();
  delete bt;

  return 0;
}
