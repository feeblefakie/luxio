#include "../btree.h"
#include <iostream>

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " dbname c|n" << std::endl; 
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

  bt->show_db_header();
  bt->show_root();

  if (!bt->close()) {
    std::cerr << "close failed" << std::endl;
  }
  delete bt;

  return 0;
}
