#include <iostream>
#include "btree.h"

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " key(string) val(int)" << std::endl; 
    exit(1);
  }

  LibMap::Btree *bt = new LibMap::Btree;
  bt->open("test", LibMap::DB_CREAT);

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[1], 256);
  uint32_t val = atoi(argv[2]);

  bt->put(key, strlen(key), &val, sizeof(uint32_t));
  bt->show_node(2);
  bt->show_node(3);

  return 0;
}
