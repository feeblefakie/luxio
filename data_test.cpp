#include <iostream>
#include "data.h"

int main(int argc, char *argv[])
{
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << "key(string)" << std::endl; 
    exit(1);
  }

  LibMap::Data *dt = new LibMap::Data();
  dt->open("datadb", LibMap::DB_CREAT);

  LibMap::data_t data = { argv[1], strlen(argv[1]) };
  dt->put(&data);

  dt->close();

  return 0;
}
