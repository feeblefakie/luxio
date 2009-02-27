#include "../../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname key(string)" << std::endl; 
    exit(1);
  }

  Lux::IO::Btree *bt = new Lux::IO::Btree(Lux::IO::NONCLUSTER);
  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "error happned" << std::endl;
    exit(-1);
  }

  char key[256];
  memset(key, 0, 256);
  memcpy(key, argv[2], 256);

  Lux::IO::data_t *val_data = bt->get(key, strlen(key));
  if (val_data != NULL) {
    std::cout << "value: " << (char *) val_data->data << std::endl;
    bt->clean_data(val_data);
  } else {
    std::cout << "[error] entry not found. [" << key << "]" << std::endl;
  } 

  return 0;
}
