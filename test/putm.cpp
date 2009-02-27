#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " dbname c|n num_records" << std::endl; 
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

  if (!bt->open(argv[1], Lux::DB_CREAT)) {
    std::cerr << "error happned" << std::endl;
    exit(-1);
  }

  int rnum = atoi(argv[3]);
  for (int i = 0; i < rnum; ++i) {
    char str[9];
    memset(str, 0, 9);
    sprintf(str,"%08d", i);
    Lux::IO::data_t data = {str, strlen(str)};

    if (!bt->put(&data, &data)) {
      std::cerr << "put failed" << std::endl;
    }
  }
  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
    exit(-1);
  }
  delete bt;

  return 0;
}
