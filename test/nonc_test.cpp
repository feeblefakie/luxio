#include "../btree.h"

#define TINY_NUM_ENTRIES 2000
#define SMALL_NUM_ENTRIES 100000
#define DEFAULT_NUM_ENTRIES 1000000
#define LARGE_NUM_ENTRIES 10000000

int num_entries_ = TINY_NUM_ENTRIES;

void put_and_cursor(Lux::DBM::Btree *bt);

int main(void)
{
  Lux::DBM::Btree *btl = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  btl->set_noncluster_params(Lux::DBM::Linked);
  if (!btl->open("tmp_linked", Lux::DB_CREAT)) {
    std::cerr << "open failed." << std::endl;
  }
  Lux::DBM::Btree *btp = new Lux::DBM::Btree(Lux::DBM::NONCLUSTER);
  btp->set_noncluster_params(Lux::DBM::Padded);
  if (!btp->open("tmp_padded", Lux::DB_CREAT)) {
    std::cerr << "open failed." << std::endl;
  }

  put_and_cursor(btl);
  put_and_cursor(btp);

  if (!btl->close()) {
    std::cerr << "close failed" << std::endl;
  }
  if (!btp->close()) {
    std::cerr << "close failed" << std::endl;
  }
  delete btl;
  delete btp;
}

void put_and_cursor(Lux::DBM::Btree *bt)
{
  for (int i = 0; i < num_entries_; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key, "%08d", i);
    // put
    if (!bt->put(key, strlen(key), &i, sizeof(int))) {
      std::cerr << "put failed." << std::endl;
    }
    // update
    if (!bt->put(key, strlen(key), &i, sizeof(int))) {
      std::cerr << "put failed." << std::endl;
    }
    // no updae
    if (!bt->put(key, strlen(key), &i, sizeof(int), Lux::DBM::NOOVERWRITE)) {
      std::cerr << "put failed." << std::endl;
    }
  }

  Lux::DBM::cursor_t *c = bt->cursor_init();
  if (c == NULL) {
    std::cerr << "cursor_init failed." << std::endl;
  }

  int num = 1000;
  char buf[9] = "00001000";
  Lux::DBM::data_t key;
  key.data = buf;
  key.size = 8;
  if (!bt->get(c, &key)) {
    std::cerr << "get failed." << std::endl;
  }
  Lux::DBM::data_t *k, *v;
  if (!bt->cursor_get(c, &k, &v, Lux::DBM::SYSTEM)) {
    std::cerr << "get failed." << std::endl;
  }
  if (num != *(int *) v->data) {
    std::cerr << "num != *(int *) v->data" << std::endl;
  }
  bt->clean_data(k);
  bt->clean_data(v);

  while (bt->next(c)) {
    Lux::DBM::data_t *key, *val;
    if (!bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM)) {
      std::cerr << "cursor_get failed" << std::endl;
    }
    ++num;
    if (num != *(int *) val->data) {
      std::cerr << "num != *(int *) v->data" << std::endl;
    }
    std::cout << "*";
    bt->clean_data(key);
    bt->clean_data(val);
  }
  std::cout << std::endl;

  num = 0;
  if (!bt->first(c)) {
    std::cerr << "first failed" << std::endl;
  }
  do {
    Lux::DBM::data_t *key, *val;
    if (!bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM)) {
      std::cerr << "cursor_get failed" << std::endl;
    }
    if (num != *(int *) val->data) {
      std::cerr << "num != *(int *) v->data" << std::endl;
    }
    ++num;
    std::cout << "-";
    bt->clean_data(key);
    bt->clean_data(val);
  } while (bt->next(c));
  std::cout << std::endl;

  num = num_entries_ - 1;
  if (!bt->last(c)) {
    std::cerr << "last failed" << std::endl;
  }
  do {
    Lux::DBM::data_t *key, *val;
    if (!bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM)) {
      std::cerr << "cursor_get failed" << std::endl;
    }
    if (num != *(int *) val->data) {
      std::cerr << "num != *(int *) v->data" << std::endl;
    }
    --num;
    std::cout << "+";
    bt->clean_data(key);
    bt->clean_data(val);
  } while (bt->prev(c));
  std::cout << std::endl;
  if (!bt->cursor_fin(c)) {
    std::cerr << "cursor_fin failed" << std::endl;
  }

}
