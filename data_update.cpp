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
  if (argc != 5) {
    std::cerr << argv[0] << " method(update|append|del) value id off" << std::endl;
    exit(-1);
  }

  double t1, t2;
  Lux::DBM::Data *dt = new Lux::DBM::PaddedData(Lux::DBM::NOPADDING);
  //Lux::DBM::Data *dt = new Lux::DBM::LinkedData(Lux::DBM::NOPADDING);
  dt->open("datadb", Lux::DB_CREAT);

  Lux::DBM::data_t data = {argv[2], strlen(argv[2])};
  Lux::DBM::data_ptr_t data_ptr_in = {atoi(argv[3]), atoi(argv[4])}; // id, off
  Lux::DBM::data_ptr_t *data_ptr;
  if (strcmp(argv[1], "update") == 0) {
    std::cout << "updating" << std::endl;
    data_ptr = dt->update(&data_ptr_in, &data);
    std::cout << data_ptr->id << "," << data_ptr->off << std::endl;
  } else if (strcmp(argv[1], "append") == 0) {
    std::cout << "appending" << std::endl;
    data_ptr = dt->append(&data_ptr_in, &data);
    std::cout << data_ptr->id << "," << data_ptr->off << std::endl;
  } else {
    std::cout << "deleting" << std::endl;
    dt->del(&data_ptr_in);
  }

  dt->show_free_pools();
  dt->show_db_header();
  dt->close();

  return 0;
}
