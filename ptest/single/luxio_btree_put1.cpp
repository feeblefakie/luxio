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

  double t1, t2;
  int rnum = atoi(argv[2]);

  t1 = gettimeofday_sec();
  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }

  //bt->set_bulk_loading(true);
  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);

    if (!bt->put(key, strlen(key), &i, sizeof(int32_t))) {
      std::cerr << "put failed." << std::endl;
    }
  }
  //bt->set_bulk_loading(false);
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  return 0;
}
