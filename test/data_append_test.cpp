#include "../data.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>

#define APPEND_LINES 100

int main(int argc, char *argv[])
{
  Lux::DBM::Data *dt = new Lux::DBM::PaddedData();
  //Lux::DBM::Data *dt = new Lux::DBM::LinkedData(Lux::DBM::RATIO, 10);
  //Lux::DBM::Data *dt = new Lux::DBM::LinkedData(Lux::DBM::NOPADDING);
  dt->open("datadb", Lux::DB_CREAT);

  std::ifstream fin;
  fin.open("./data.txt", std::ios::in);
  if (!fin) {
    std::cout << "cannot open the file" << std::endl;
    exit(-1);
  }

  std::vector<Lux::DBM::data_ptr_t *> vec;
  Lux::DBM::data_ptr_t *data_ptr;
  std::string line;
  std::string chunk;
  int i = 0;
  while (getline(fin, line)) {
    Lux::DBM::data_t data = {line.c_str(), line.size()};
    if (i == 0) {
      data_ptr = dt->put(&data);
    } else if (i % APPEND_LINES == 0) {
      if (argc == 2 && (random() % 3) == 2) {
        dt->del(data_ptr);
      } else {
        vec.push_back(data_ptr); 
        std::cout << chunk << std::endl;
        //std::cout << i << std::endl;
      }
      chunk.clear();
      data_ptr = dt->put(&data);
    } else {
      data_ptr = dt->append(data_ptr, &data);
    }
    chunk += line;
    ++i;
  }
  fin.close();

  std::vector<Lux::DBM::data_ptr_t *>::iterator itr = vec.begin();
  std::vector<Lux::DBM::data_ptr_t *>::iterator itr_end = vec.end();

  while (itr != itr_end) {
    Lux::DBM::data_t *data = dt->get(*itr);
    std::cerr.write((char *) data->data, data->size);
    std::cerr << std::endl;
    ++itr;
  }

  //dt->show_free_pools();
  //dt->show_db_header();
  dt->close();
  delete dt;

  return 0;
}
