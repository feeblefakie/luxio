#include <iostream>
#include "btree.h"

int main(void)
{
  LibMap::Btree *bt = new LibMap::Btree;

  bt->open("test", LibMap::DB_CREAT);

  char key[256];
  uint32_t val = 1980;
  memset(key, 0, 256);
  strncpy(key, "hello", 5);

  bt->put(key, strlen(key), &val, sizeof(uint32_t));
  bt->show_node(2);

  return 0;
}
