#include <iostream>
#include "btree.h"

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  LibMap::Btree *bt = new LibMap::Btree;
  bt->open(argv[1], LibMap::DB_CREAT);

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[2], 256);

  bt->del(key, strlen(key));
  bt->show_node();

  return 0;
}
