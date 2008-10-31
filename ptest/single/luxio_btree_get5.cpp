#include "../../btree.h"
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
    std::cerr << "Usage: " << argv[0] << " dbname record_num" << std::endl; 
    exit(1);
  }
  int rnum = atoi(argv[2]);

  double t1, t2;
  t1 = gettimeofday_sec();
  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  if (!bt->open(argv[1], Lux::DB_RDONLY)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    char *val = new char[307201];
    sprintf(val, "%0102400d", i);
    sprintf(val + 102400, "%0102400d", i);
    sprintf(val + 102400*2, "%0102400d", i);

    Lux::DBM::data_t key_data = {key, strlen(key)};
    Lux::DBM::data_t *val_data = bt->get(&key_data);
    if (val_data != NULL) {
      if (strcmp(val, (char *) val_data->data) != 0) {
        std::cout << "[error] value incorrect." << (char *) val_data->data << " expected [" << val << "]" << std::endl;
      }
      bt->clean_data(val_data);
    } else {
      std::cout << "[error] entry not found for [" << key << "]" << std::endl;
    } 
    delete [] val;
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  return 0;
}
