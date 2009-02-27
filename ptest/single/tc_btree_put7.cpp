#include <tcutil.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
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
int main(int argc, char **argv){

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

  int ecode;
  char *key, *value;

  t1 = gettimeofday_sec();
  TCBDB *bdb = tcbdbnew();
  tcbdbtune(bdb, 5, -1, -1, -1, -1, 0);
  //tcbdbtune(bdb, 240, -1, -1, -1, -1, 0);
  //tcbdbtune(bdb, 480, -1, -1, -1, -1, BDBTDEFLATE);
  tcbdbsetcache(bdb, 300, 100);
  if (!tcbdbopen(bdb, argv[2], BDBOWRITER | BDBOCREAT)) {
    ecode = tcbdbecode(bdb);
    fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
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

    if (!tcbdbput(bdb, key, strlen(key), val, strlen(val))) {
      fprintf(stderr, "put error\n");
    }
    delete [] val;
    memset(buf, 0, 256);
  }
  rewind(fp);
  while (fgets(buf, 256, fp)) {
    char *key = strchr(buf, ' ');
    if (key == NULL) { continue; }
    *key++ = '\0';
    char *valp = strchr(key, ' ');
    if (valp == NULL) { continue; }
    *valp++ = '\0';
    char *val = new char[102401]; // 100K
    sprintf(val, "%0102400d", atoi(valp));

    if (!tcbdbputcat(bdb, key, strlen(key), val, strlen(val))) {
      fprintf(stderr, "put error\n");
    }
    delete [] val;
    memset(buf, 0, 256);
  }
  rewind(fp);
  while (fgets(buf, 256, fp)) {
    char *key = strchr(buf, ' ');
    if (key == NULL) { continue; }
    *key++ = '\0';
    char *valp = strchr(key, ' ');
    if (valp == NULL) { continue; }
    *valp++ = '\0';
    char *val = new char[102401]; // 100K
    sprintf(val, "%0102400d", atoi(valp));

    if (!tcbdbputcat(bdb, key, strlen(key), val, strlen(val))) {
      fprintf(stderr, "put error\n");
    }
    delete [] val;
    memset(buf, 0, 256);
  }

  if (!tcbdbclose(bdb)) {
    ecode = tcbdbecode(bdb);
    fprintf(stderr, "close error: %s\n", tcbdberrmsg(ecode));
  }
  tcbdbdel(bdb);
  t2 = gettimeofday_sec();
  std::cout << "put time: " << t2 - t1 << std::endl;

  fclose(fp);

  return 0;
}
