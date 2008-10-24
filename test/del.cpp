#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[1], Lux::DB_RDWR)) {
    std::cerr << "open failed" << std::endl;
    exit(-1);
  }

  Lux::DBM::data_t key = {argv[2], strlen(argv[2])};
  if (!bt->del(&key)) {
    std::cerr << "del failed" << std::endl;  
  }

  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
    exit(-1);
  }
  delete bt;

  return 0;
}