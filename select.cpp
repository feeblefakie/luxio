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

  bt->get(key, strlen(key));
  LibMap::data_t *val_data = bt->get(key, strlen(key));
  if (val_data != NULL) {
    int val;
    memcpy(&val, val_data->data, val_data->size);
    std::cout << "value: " << val << std::endl;
    bt->clean_data(val_data);
  } else {
    std::cout << "[error] entry not found. [" << key << "]" << std::endl;
  } 

  return 0;
}
