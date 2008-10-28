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
  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[2], Lux::DB_CREAT)) {
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
    int32_t val = atoi(valp);

    if (!bt->put(key, strlen(key), &val, sizeof(int32_t))) {
      std::cerr << "put failed." << std::endl;
    }
    memset(buf, 0, 256);
  }
  bt->close();
  delete bt;

  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  return 0;
}
