#include "data2.h"
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
  LibMap::Data *dt = new LibMap::Data();
  dt->open("datadb", LibMap::DB_CREAT);

  std::ifstream fin;
  fin.open("./data.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }

  t1 = gettimeofday_sec();
  //std::vector<LibMap::data_ptr_t *> vec;
  std::string line;
  while (getline(fin, line)) {
    LibMap::data_t data = {line.c_str(), line.size()};
    LibMap::data_ptr_t *data_ptr = dt->put(&data);
    //vec.push_back(data_ptr);
    std::cerr << data_ptr->id << "," << data_ptr->off << std::endl;
    dt->clean_data_ptr(data_ptr);
  }
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  fin.close();

/*
  fin.open("./data.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }
  std::vector<LibMap::data_ptr_t *>::iterator itr = vec.begin();
  std::vector<LibMap::data_ptr_t *>::iterator itr_end = vec.end();

  t1 = gettimeofday_sec();
  while (getline(fin, line)) {
    LibMap::data_t *data = dt->get(*itr); 
    //std::cout << "data: [";
    //std::cout.write(data->>data, data->size);
    //std::cout << "]" << std::endl;;
    if (data->size != line.size() ||
        strncmp((char *) data->data, line.c_str(), data->size) != 0) {
      std::cout << "ERROR: GOT WRONG DATA" << std::endl;
    } else {
      //std::cout << "data [ok]" << std::endl;
    }
    dt->clean_data(data); 
    dt->clean_data_ptr(*itr); // data_ptr
    ++itr;
  }
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  fin.close();
*/

  dt->show_free_pools();
  dt->show_db_header();

  dt->close();

  return 0;
}
