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
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " method(update|del) index val(uint32_t)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Array *ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
  ary->set_noncluster_params(Lux::DBM::Padded);
  ary->open("anoc", Lux::DB_CREAT);

  int index = atoi(argv[2]);
  double t1, t2;

  if (strcmp(argv[1], "update") == 0) {
    std::cout << "index: " << index << std::endl;
    Lux::DBM::data_t *data = ary->get(index);
    if (data != NULL) {
      std::cout << "val: " << *(uint32_t *) data->data << std::endl;
      ary->clean_data(data);
    } else {
      std::cout << "returned NULL" << std::endl;
    }

    Lux::DBM::data_t idata;
    uint32_t val = atoi(argv[3]);
    idata.data = &val;
    idata.size = sizeof(uint32_t);
    ary->put(index, &idata);

    data = ary->get(index);
    if (data != NULL) {
      std::cout << "val: " << *(uint32_t *) data->data << std::endl;
      ary->clean_data(data);
    } else {
      std::cout << "returned NULL" << std::endl;
    }

  } else if (strcmp(argv[1], "del") == 0) {
    Lux::DBM::data_t *data = ary->get(index);
    if (data != NULL) {
      //std::cout << "before: " << *(uint32_t *) data->data << std::endl;
      std::cout.write((char *) data->data, data->size); 
      std::cout << std::endl;
      ary->clean_data(data);
    } else {
      std::cout << "returned NULL" << std::endl;
    }

    ary->del(index);
     
    data = ary->get(index);
    if (data != NULL) {
      std::cout << "after: " << *(uint32_t *) data->data << std::endl;
      ary->clean_data(data);
    } else {
      std::cout << "returned NULL" << std::endl;
    }
  }

  ary->close();
  delete ary;

  return 0;
}
