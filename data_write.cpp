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
  if (argc != 2) {
    std::cerr << argv[0] << " value" << std::endl;
    exit(-1);
  }

  double t1, t2;
  LibMap::Data *dt = new LibMap::PaddedData(LibMap::FIXEDLEN);
  //LibMap::Data *dt = new LibMap::LinkedData(LibMap::FIXEDLEN);
  dt->open("datadb", LibMap::DB_CREAT);

  LibMap::data_t data = {argv[1], strlen(argv[1])};
  LibMap::data_ptr_t *data_ptr = dt->put(&data);
  std::cout << data_ptr->id << "," << data_ptr->off << std::endl;

  dt->show_free_pools();
  dt->show_db_header();
  dt->close();

  return 0;
}
