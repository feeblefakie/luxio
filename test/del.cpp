#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname c|n key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt;
  if (strcmp(argv[2], "c") == 0) { 
     bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER, 8);
  } else if (strcmp(argv[2], "n") == 0) {
     bt = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  } else {
    std::cerr << "select c or n" << std::endl;
    exit(-1);
  }

  if (!bt->open(argv[1], Lux::DB_RDWR)) {
    std::cerr << "open failed" << std::endl;
    exit(-1);
  }

  Lux::DBM::data_t key = {argv[3], strlen(argv[3])};
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
