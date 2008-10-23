#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[1], Lux::DB_RDONLY)) {
    std::cerr << "open failed" << std::endl;
    exit(-1);
  }

  Lux::DBM::data_t key = {argv[2], strlen(argv[2])};
  Lux::DBM::data_t *val = bt->get(&key);
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
