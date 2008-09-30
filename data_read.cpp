#include "data.h"
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
  Lux::DBM::Data *dt = new Lux::DBM::PaddedData();
  //Lux::DBM::Data *dt = new Lux::DBM::LinkedData();
  if (!dt->open("datadb", Lux::DB_CREAT)) {
    std::cerr << "open failed" << std::endl;
    dt->close();
    exit(-1);
  }

  t1 = gettimeofday_sec();
  Lux::DBM::data_ptr_t data_ptr = {atoi(argv[1]), atoi(argv[2])};
  Lux::DBM::data_t *data = dt->get(&data_ptr);
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
