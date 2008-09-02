#include <iostream>
#include "btree.h"

int main(void)
{
  LibMap::Btree *bt = new LibMap::Btree;

  bt->open("test", LibMap::DB_CREAT);

  return 0;
}
