#include "array.h"
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
  double t1, t2;
  Lux::DBM::Array *ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
  ary->set_noncluster_params(Lux::DBM::Padded, Lux::DBM::NOPADDING);
  ary->open("anoc", Lux::DB_CREAT);

  std::ifstream fin;
  fin.open("./data.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }

  t1 = gettimeofday_sec();
  std::vector<Lux::DBM::data_ptr_t *> vec;
  std::string line;
  int i = 0;
  while (getline(fin, line)) {
    Lux::DBM::data_t data = {line.c_str(), line.size()};
    if (!ary->put(i++, &data)) {
      std::cerr << "put failed" << std::endl;
    }
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  fin.close();

  fin.open("./data.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }
  std::vector<Lux::DBM::data_ptr_t *>::iterator itr = vec.begin();
  std::vector<Lux::DBM::data_ptr_t *>::iterator itr_end = vec.end();

  Lux::DBM::data_t data;
  char buf[4096];
  data.data = buf;
  data.size = 4096;
  uint32_t size;

  t1 = gettimeofday_sec();
  i = 0;
  while (getline(fin, line)) {
    if (ary->get(i++, &data, &size)) {
      if (size != line.size() ||
          strncmp((char *) data.data, line.c_str(), size) != 0) {
        std::cout << "ERROR: GOT WRONG DATA - size: " << size << ", line size: " << line.size() << std::endl;
      } else {
        //std::cout << "data [ok]" << std::endl;
      }
    }

    /*
    Lux::DBM::data_t *data = ary->get(*itr); 
    //std::cout << "data: [";
    //std::cout.write(data->>data, data->size);
    //std::cout << "]" << std::endl;;
    if (data->size != line.size() ||
        strncmp((char *) data->data, line.c_str(), data->size) != 0) {
      std::cout << "ERROR: GOT WRONG DATA" << std::endl;
    } else {
      //std::cout << "data [ok]" << std::endl;
    }
    ary->clean_data(data); 
    */
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  fin.close();

  ary->show_db_header();

  ary->close();

  delete ary;

  return 0;
}
