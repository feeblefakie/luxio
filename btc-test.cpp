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
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " dbname" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  //bt->set_lock_type(Lux::DBM::LOCK_PROCESS);
  //bt->set_page_size(4096);
  bt->open(argv[1], Lux::DB_CREAT);

  Lux::DBM::cursor_t *c = bt->cursor_init();
  bt->first(c); 
  while (bt->next(c)) {
    //bt->print_cursor(c);
    bt->cursor_get(c);
    //sleep(1);
    //usleep(10000);
  }
  bt->cursor_fin(c);

  bt->close();
  delete bt;

  return 0;
}
