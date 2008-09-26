#include <iostream>
#include "btree.h"

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree;
  bt->open(argv[1], Lux::DB_CREAT);

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[2], 256);

  bt->del(key, strlen(key));
  bt->show_node();

  return 0;
}
