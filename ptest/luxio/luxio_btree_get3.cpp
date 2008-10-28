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
    std::cerr << "Usage: " << argv[0] << " file dbname usermem(1/0)" << std::endl; 
    exit(1);
  }

  double t1, t2;
  FILE *fp = fopen(argv[1], "r");
  if (fp == NULL) {
    std::cerr << "failed to open " << argv[1] << std::endl;
    exit(1);
  }

  bool usermem = false;
  if (atoi(argv[3]) == 1) {
    usermem = true;
  }

  t1 = gettimeofday_sec();
  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[2], Lux::DB_RDONLY)) {
    std::cerr << "open failed" << std::endl;
    exit(1);
  }

  char buf[256];
  while (fgets(buf, 256, fp)) {
    char *key = strchr(buf, ' ');
    if (key == NULL) { continue; }
    *key++ = '\0';
    char *valp = strchr(key, ' ');
    if (valp == NULL) { continue; }
    *valp++ = '\0';
    int32_t aval = atoi(valp);

    Lux::DBM::data_t key_data = {key, strlen(key)};
    if (usermem) {
      int32_t val;
      Lux::DBM::data_t val_data = {&val, 0, sizeof(int32_t)};
      Lux::DBM::data_t *val_p = &val_data;

      if (bt->get(&key_data, &val_p, Lux::DBM::USER)) {
        if (val != aval) {
          std::cout << "[error] value incorrect." << std::endl;
        }
      } else {
        std::cout << "[error] entry not found for [" << key << "]" << std::endl;
      } 
    } else {
      Lux::DBM::data_t *val_data = bt->get(&key_data);
      if (val_data != NULL) {
        if (*(int32_t *) val_data->data != aval) {
          std::cout << "[error] value incorrect." << std::endl;
        }
        bt->clean_data(val_data);
      } else {
        std::cout << "[error] entry not found for [" << key << "]" << std::endl;
      } 
    }
    memset(buf, 0, 256);
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  fclose(fp);

  return 0;
}
