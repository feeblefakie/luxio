#include <luxio/btree.h>
#include <iostream>

#define NUM_RECORDS 1000000

int main(void)
{
  Lux::IO::Btree *bt = new Lux::IO::Btree(Lux::IO::CLUSTER);
  bt->open("test", Lux::IO::DB_RDONLY);

  char str[9];
  for (int i = 0; i < NUM_RECORDS; ++i) {
    sprintf(str, "%08d", i);

    Lux::IO::data_t key = {str, strlen(str)};
    Lux::IO::data_t *val = bt->get(&key); // select operation

    // do something with the val
    // *(int *) val->data
    
    bt->clean_data(val);
  }
  bt->close();
  delete bt;
  
  return 0;
}
