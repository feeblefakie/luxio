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

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER, sizeof(char)*8);
  //Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER, sizeof(int));
  // !!! this is needed !!1
  bt->set_cmp_func(Lux::DBM::int32_cmp_func);
  bt->open("intbenchdb", Lux::DB_CREAT);

  int rnum = atoi(argv[1]);
  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);

      bt->put(&i, sizeof(int), key, strlen(key));
      //bt->put(&i, sizeof(int), &i, sizeof(int));
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
   
    if (argc != 3) {
      return 0;
    }
  }
  bt->show_db_header();

  if (mode == 2 || mode == 3) {

    int select_num = rnum / 10;
    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);

      Lux::DBM::data_t *val_data = bt->get(&i, sizeof(int));
      if (val_data != NULL) {
        ///*
        char val[9];
        memset(val, 0, 9);
        memcpy(&val, val_data->data, val_data->size);
        if (strcmp(key, val) != 0) {
          std::cout << "[error] value incorrect. " << "[" << key << "]" << val_data->size << " : " << val << std::endl;
        }
        //*/
        /*
        int num;
        memcpy(&num, val_data->data, val_data->size);
        if (i != num) {
          std::cout << "[error] value incorrect. " << "[" << i << "]" << val_data->size << " : " << num << std::endl;
        }
        */
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
