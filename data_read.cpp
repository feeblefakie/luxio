#include "data3.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
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
  if (argc != 3) {
    std::cerr << argv[0] << " id off" << std::endl;
    exit(-1);
  }

  double t1, t2;
  LibMap::Data *dt = new LibMap::PaddedData();
  //LibMap::Data *dt = new LibMap::LinkedData();
  dt->open("datadb", LibMap::DB_CREAT);

  t1 = gettimeofday_sec();
  LibMap::data_ptr_t data_ptr = {atoi(argv[1]), atoi(argv[2])};
  LibMap::data_t *data = dt->get(&data_ptr);
  if (data != NULL) {
    std::cout << "HIT" << std::endl;
    std::cout.write((char *) data->data, data->size);
    std::cout << std::endl;
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  //dt->show_free_pools();
  //dt->show_db_header();
  dt->close();

  return 0;
}
