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
    std::cerr << "Usage: " << argv[0] << " file dbname" << std::endl; 
    exit(1);
  }

  double t1, t2;
  FILE *fp = fopen(argv[1], "r");
  if (fp == NULL) {
    std::cerr << "failed to open " << argv[1] << std::endl;
    exit(1);
  }

  t1 = gettimeofday_sec();
  Lux::IO::Btree *bt = new Lux::IO::Btree(Lux::IO::NONCLUSTER);
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
    char *val = new char[102401]; // 100K
    sprintf(val, "%0102400d", atoi(valp));

    Lux::IO::data_t key_data = {key, strlen(key)};
    Lux::IO::data_t *val_data = bt->get(&key_data);
    if (val_data != NULL) {
      if (strcmp(val, (char *) val_data->data) != 0) {
        std::cout << "[error] value incorrect." << (char *) val_data->data << " expected [" << val << "]" << std::endl;
      }
      bt->clean_data(val_data);
    } else {
      std::cout << "[error] entry not found for [" << key << "]" << std::endl;
    } 
    delete [] val;
    memset(buf, 0, 256);
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "get time: " << t2 - t1 << std::endl;

  fclose(fp);

  return 0;
}
