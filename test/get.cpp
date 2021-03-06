#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname c|n key(string)" << std::endl; 
    exit(1);
  }

  Lux::IO::Btree *bt;
  if (strcmp(argv[2], "c") == 0) { 
     bt = new Lux::IO::Btree(Lux::IO::CLUSTER, 8);
  } else if (strcmp(argv[2], "n") == 0) {
     bt = new Lux::IO::Btree(Lux::IO::NONCLUSTER);
  } else {
    std::cerr << "select c or n" << std::endl;
    exit(-1);
  }

  if (!bt->open(argv[1], Lux::DB_RDONLY)) {
    std::cerr << "open failed" << std::endl;
    exit(-1);
  }

  Lux::IO::data_t key = {argv[3], strlen(argv[3])};
  Lux::IO::data_t *val = bt->get(&key);
  if (val != NULL) {
    std::cout << "hit: [";
    std::cout.write((char *) val->data, val->size);
    std::cout << "]";
    std::cout << std::endl;
    bt->clean_data(val);
  } else {
    std::cout << "[error] entry not found. [" << argv[2] << "]" << std::endl;
  } 

  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
  }
  delete bt;

  return 0;
}
