#include "array.h"
#include <iostream>
#include <vector>
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

  Lux::DBM::Array *ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
  ary->open("arraydb", Lux::DB_CREAT);

  int rnum = atoi(argv[1]);
  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      ary->put(i, &i, sizeof(uint32_t));
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
   
    if (argc != 3) {
      return 0;
    }
  }

  if (mode == 2 || mode == 3) {

    //std::vector<uint32_t> vec;
    //vec.reserve(rnum);
    t1 = gettimeofday_sec();
    for (int i = 0; i < rnum; ++i) {
      //Lux::DBM::data_t *val_data = ary->get(i);
      Lux::DBM::data_t data;
      uint32_t val, size;
      data.data = &val;
      ary->get(i, &data, &size);
      //vec.push_back(data->data);
      // (*(uint32_t *) data.data
      if (val != i) {
        std::cout << "[error] value incorrect. i=" << i << ", data=" << val << std::endl;
      }

      //if (val_data != NULL) {
        /*
        uint32_t val;
        memcpy(&val, val_data->data, val_data->size);
        if (val != i) {
          std::cout << "[error] value incorrect." << std::endl;
        }
        */
        //ary->clean_data(val_data);
      /*
      } else {
        std::cout << "[error] entry not found. [" << i << "]" << std::endl;
      } 
      */
    }
    t2 = gettimeofday_sec();
    std::cout << "get time: " << t2 - t1 << std::endl;
  }

  ary->show_db_header();

  ary->close();
  delete ary;

  return 0;
}
