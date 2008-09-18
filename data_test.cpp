#include <iostream>
#include "data.h"

int main(int argc, char *argv[])
{
  /*
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << "key(string)" << std::endl; 
    exit(1);
  }
  */

  LibMap::Data *dt = new LibMap::Data();
  dt->open("datadb", LibMap::DB_CREAT);

  char val[5000];
  memset(val, 0, 5000);
  memset(val, 'a', 4999);

  LibMap::data_t data;
  if (argc != 2) {
    data.data = val;
    data.size = strlen(val); 
  } else {
    data.data = argv[1];
    data.size = strlen(argv[1]);
  }
  //LibMap::data_t data = { argv[1], strlen(argv[1]) };
  //LibMap::data_t data = { val, strlen(val) };
  LibMap::data_ptr_t *data_ptr = dt->put(&data);
  std::cout << "\n=== RESULT ===" << std::endl;
  std::cout << "block id: " << data_ptr->id << std::endl;
  std::cout << "offset: " << data_ptr->off << std::endl;
  std::cout << "=== RESULT ===\n" << std::endl;

  dt->get(data_ptr); 

  dt->show_free_pools();

  dt->close();

  return 0;
}
