#include <iostream>
#include "btree.h"

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string) val(int)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  bt->open(argv[1], Lux::DB_CREAT);

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[2], 256);
  uint32_t val = atoi(argv[3]);

  bt->put(key, strlen(key), &val, sizeof(uint32_t));
  bt->show_node();

  bt->close();

  return 0;
}
