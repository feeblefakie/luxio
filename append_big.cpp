#include "data3.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

#define BUFSIZE 1024*1024*10

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

int main(int argc, char *argv[])
{
  char *p = new char[BUFSIZE];
  memset(p, 1, BUFSIZE);
  double t1, t2;
  //LibMap::Data *dt = new LibMap::PaddedData(LibMap::NOPADDING);
  //LibMap::Data *dt = new LibMap::LinkedData(LibMap::NOPADDING);
  LibMap::Data *dt = new LibMap::LinkedData(LibMap::RATIO, 50);
  dt->open("datadb", LibMap::DB_CREAT);

  LibMap::data_t data = {p, BUFSIZE};
  t1 = gettimeofday_sec();
  LibMap::data_ptr_t *data_ptr = dt->put(&data);
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  std::cout << "appending" << std::endl;
  t1 = gettimeofday_sec();
  data_ptr = dt->append(data_ptr, &data);
  t2 = gettimeofday_sec();
  std::cout << "append time: " << t2 - t1 << std::endl;

  dt->show_free_pools();
  dt->show_db_header();
  dt->close();

  return 0;
}
