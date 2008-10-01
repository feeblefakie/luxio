#include "btree.h"
#include <iostream>
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
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "error happned" << std::endl;
    exit(-1);
  }

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[2], 256);

  double t1, t2;
  t1 = gettimeofday_sec();
  bt->get(key, strlen(key));
  Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
  if (val_data != NULL) {
    std::cout.write((char *) val_data, val_data->size);
    bt->clean_data(val_data);
  } else {
    std::cout << "[error] entry not found. [" << key << "]" << std::endl;
  } 
  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  bt->close();
  delete bt;

  return 0;
}
