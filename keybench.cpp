#include <iostream>
#include "btree.h"
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <fstream>

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

int main(int argc, char *argv[])
{
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " select?" << std::endl; 
    exit(1);
  }
  int mode;
  if(argc == 2) {
    mode = atoi(argv[1]);
  }

  std::ifstream fin;
  fin.open("./tokens.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }

  LibMap::Btree *bt = new LibMap::Btree(LibMap::CLUSTER, sizeof(int));
  bt->open("keybenchdb", LibMap::DB_CREAT);

  double t1, t2;

  if (mode == 1 || mode == 3) {

    t1 = gettimeofday_sec();
    std::string line;
    while (getline(fin, line)) {

      int size = line.size();
      bt->put(line.c_str(), size, &size, sizeof(int));
    }
    t2 = gettimeofday_sec();
    std::cout << "put time: " << t2 - t1 << std::endl;
   
  }

  fin.close();
  fin.open("./tokens.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }

  if (mode == 2 || mode == 3) {

    t1 = gettimeofday_sec();
    std::string line;
    while (getline(fin, line)) {

      LibMap::data_t *val_data = bt->get(line.c_str(), line.size());
      if (val_data != NULL) {
        int val;
        memcpy(&val, val_data->data, val_data->size);
        if (line.size() != val) {
          std::cout << "[error] value incorrect." << std::endl;
        }
        bt->clean_data(val_data);
      } else {
        std::cout << "[error] entry not found. [" << line << "]" << std::endl;
      } 
    }
    t2 = gettimeofday_sec();
    std::cout << "get time: " << t2 - t1 << std::endl;
  }

  bt->show_db_header();

  bt->close();
  fin.close();

  return 0;
}
