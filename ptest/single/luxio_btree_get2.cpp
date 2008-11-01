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
    std::cerr << "Usage: " << argv[0] << " dbname record_num usermem[1/0]" << std::endl; 
    exit(1);
  }
  int rnum = atoi(argv[2]);
  bool usermem = false;
  if (atoi(argv[3]) == 1) {
    usermem = true;
  }

  double t1, t2;
  t1 = gettimeofday_sec();
  Lux::IO::Btree *bt = new Lux::IO::Btree(Lux::IO::CLUSTER);
  if (!bt->open(argv[1], Lux::DB_RDONLY)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    char val[65];
    memset(val, 0, 65);
    sprintf(val, "%064d", i);

    Lux::IO::data_t key_data = {key, strlen(key)};
    if (usermem) {
      char rval[65];
      memset(rval, 0, 65);
      Lux::IO::data_t val_data = {rval, 0, 65};
      Lux::IO::data_t *val_p = &val_data;

      if (bt->get(&key_data, &val_p, Lux::IO::USER)) {
        if (strcmp(val, (char *) val_p->data) != 0) {
          std::cout << "[error] value incorrect." << std::endl;
        }
      } else {
        std::cout << "[error] entry not found for [" << key << "]" << std::endl;
      } 
    } else {
      Lux::IO::data_t *val_data = bt->get(&key_data);
      if (val_data != NULL) {
        if (strcmp(val, (char *) val_data->data) != 0) {
          std::cout << "[error] value incorrect." << std::endl;
        }
        bt->clean_data(val_data);
      } else {
        std::cout << "[error] entry not found for [" << key << "]" << std::endl;
      } 
    }
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  return 0;
}
