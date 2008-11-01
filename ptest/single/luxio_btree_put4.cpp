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
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname mode(linked:0,padded:1) record_num" << std::endl; 
    exit(1);
  }

  double t1, t2;
  int mode = atoi(argv[2]);
  int rnum = atoi(argv[3]);

  t1 = gettimeofday_sec();
  Lux::IO::Btree *bt = new Lux::IO::Btree(Lux::IO::NONCLUSTER);
  if (mode == 0) {
    bt->set_noncluster_params(Lux::IO::Linked);
  } else {
    bt->set_noncluster_params(Lux::IO::Padded);
  }
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    char *val = new char[102401]; // 100K
    sprintf(val, "%0102400d", i);
    Lux::IO::data_t key_data = {key, strlen(key)};
    Lux::IO::data_t val_data = {val, strlen(val)};

    if (!bt->put(&key_data, &val_data)) {
      std::cerr << "put failed." << std::endl;
    }
    delete [] val;
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  return 0;
}
