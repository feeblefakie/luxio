#include <iostream>
#include "btree.h"

int main(int argc, char *argv[])
{
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " record_num" << std::endl; 
    exit(1);
  }

  LibMap::Btree *bt = new LibMap::Btree;
  bt->open("benchdb", LibMap::DB_CREAT);

  int rnum = atoi(argv[1]);

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    //std::cout << "[" << key << "]" << std::endl;

    bt->put(key, strlen(key), &i, sizeof(uint32_t));
  }
  std::cout << "stored done." << std::endl;
  std::cout << "selecting ..." << std::endl;

  for (int i = 0; i < rnum; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);

    LibMap::data_t *val_data = bt->get(key, strlen(key));
    if (val_data != NULL) {
      uint32_t val;
      memcpy(&val, val_data->data, val_data->size);
      if (i != val) {
        std::cout << "[error] value incorrect." << std::endl;
      }
    } else {
      std::cout << "[error] entry not found." << std::endl;
    }  
  }

  return 0;
}
