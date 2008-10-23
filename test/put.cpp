#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string) val(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER, 8);
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "error happned" << std::endl;
    exit(-1);
  }

  Lux::DBM::data_t key = {argv[2], strlen(argv[2])};
  Lux::DBM::data_t val = {argv[3], strlen(argv[3])};

  if (!bt->put(&key, &val)) {
    std::cerr << "put failed" << std::endl;
  }
  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
    exit(-1);
  }
  delete bt;

  return 0;
}
