#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::DBM::Btree *bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "error happned" << std::endl;
    exit(-1);
  }

  Lux::DBM::data_t key = {argv[2], strlen(argv[2])};
  Lux::DBM::data_t *val = bt->get(&key);
  if (val != NULL) {
    std::cout << "hit: [";
    std::cout.write(val->data, val->size);
    std::cout << std::endl;
    bt->clean_data(val_data);
  } else {
    std::cout << "[error] entry not found. [" << key << "]" << std::endl;
  } 

  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
  }
  delete bt;

  return 0;
}
