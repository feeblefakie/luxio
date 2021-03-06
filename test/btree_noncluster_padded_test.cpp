#include "../btree.h"
#include <gtest/gtest.h>

#define TINY_NUM_ENTRIES 2000
#define SMALL_NUM_ENTRIES 100000
#define DEFAULT_NUM_ENTRIES 1000000
#define LARGE_NUM_ENTRIES 10000000

namespace {
  
  static uint32_t db_num_ = 0;

  class BtreeTest : public testing::Test {
  protected:  
    virtual void SetUp()
    {
      bt = new Lux::IO::Btree(Lux::IO::NONCLUSTER);
      db_name_ = "btncp_test";
      num_entries_ = SMALL_NUM_ENTRIES;
    }
    virtual void TearDown()
    {
      delete bt;
      bt = NULL;
    }
    std::string get_db_name(uint32_t num)
    {
      char buf[256];
      memset(buf, 0, 256);
      sprintf(buf, "%d", num);
      std::string db_name = db_name_ + buf; 
      return db_name;
    }
   
    Lux::IO::Btree *bt; 
    std::string db_name_;
    uint32_t num_entries_;
  };

  /* 
   * operation: put
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   **/
  TEST_F(BtreeTest, PutPaddedTest) {
    bt->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      // put
      ASSERT_EQ(true, bt->put(key, strlen(key),
                &i, sizeof(int)));
      // update
      ASSERT_EQ(true, bt->put(key, strlen(key),
                &i, sizeof(int)));
      // no updae
      ASSERT_EQ(true, bt->put(key, strlen(key),
                &i, sizeof(int), Lux::IO::NOOVERWRITE));
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: get
   * sequence: ordered
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, GetPaddedTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      // sequential, system memory
      Lux::IO::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      bt->clean_data(val_data);

      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      // random, system memory
      val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(num == *(int *) val_data->data);
      bt->clean_data(val_data);

      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      int val;
      Lux::IO::data_t key_data = {key, strlen(key)};
      Lux::IO::data_t val_data2 = {&val, 0, sizeof(int)};
      Lux::IO::data_t *val_p = &val_data2;

      // sequntial, user memory
      ASSERT_EQ(true, bt->get(&key_data, &val_p));
      ASSERT_TRUE(i == val);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: cursor
   * sequence: random
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, CursorPaddedTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    char buf[9] = "00001000";
    Lux::IO::data_t key;
    int num = 1000;
    key.data = buf;
    key.size = 8;
    Lux::IO::cursor_t *c = bt->cursor_init();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(true, bt->get(c, &key));

    while (bt->next(c)) {
      Lux::IO::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::IO::SYSTEM));
      ++num;
      ASSERT_EQ(num, *(int *) val->data);
      bt->clean_data(key);
      bt->clean_data(val);
    }

    num = 0;
    ASSERT_EQ(true, bt->first(c));
    do {
      Lux::IO::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::IO::SYSTEM));
      ASSERT_EQ(num, *(int *) val->data);
      ++num;
      bt->clean_data(key);
      bt->clean_data(val);
    } while (bt->next(c));
    ASSERT_EQ(num, num_entries_);

    num = num_entries_ - 1;
    ASSERT_EQ(true, bt->last(c));
    do {
      Lux::IO::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::IO::SYSTEM));
      ASSERT_EQ(num, *(int *) val->data);
      --num;
      bt->clean_data(key);
      bt->clean_data(val);
    } while (bt->prev(c));
    ASSERT_EQ(true, bt->cursor_fin(c));

    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: del
   * key: string
   * val: binary
   * omode: rdwr
   **/
  TEST_F(BtreeTest, DelPaddedTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDWR));

    for (int i = 0; i < num_entries_; ++i) {
      if (i % 2 == 0) {
        continue;
      }
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      Lux::IO::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      bt->clean_data(val_data);
      ASSERT_EQ(true, bt->del(key, strlen(key)));
      val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(bt->get(key, strlen(key)) == NULL);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put, append
   * sequence: ordered
   * key: string
   * val: string
   * omode: create
   **/
  TEST_F(BtreeTest, AppendPaddedTest) {
    bt->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[41];
      memset(key, 0, 41);
      sprintf(key, "%040d", i);
      // put
      ASSERT_EQ(true, bt->put(key, strlen(key), key, strlen(key)));
      // append
      ASSERT_EQ(true, bt->put(key, strlen(key), key, strlen(key), Lux::IO::APPEND));
    }

    ASSERT_EQ(true, bt->close()); 
  }

  /* 
   * operation: get
   * sequence: ordered
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, GetAppendedPaddedTest) {
    bt->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[41];
      memset(key, 0, 41);
      sprintf(key, "%040d", i);
      std::string correct = std::string(key) + std::string(key);

      // sequential, system memory
      Lux::IO::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
      ASSERT_EQ(correct.size(), val_data->size);
      bt->clean_data(val_data);
      
      // user memory
      char val[81];
      memset(val, 0, 81);
      Lux::IO::data_t key_data = {key, strlen(key)};
      Lux::IO::data_t val_data2 = {val, 0, 81};
      Lux::IO::data_t *val_p = &val_data2;

      ASSERT_EQ(true, bt->get(&key_data, &val_p));
      ASSERT_EQ(correct.size(), val_p->size);
      ASSERT_TRUE(strcmp((char *) val_p->data, correct.c_str()) == 0);

      memset(val, 0, 81);
      Lux::IO::data_t val_data3 = {val, 0, 80}; // no NULL space
      val_p = &val_data3;

      ASSERT_EQ(true, bt->get(&key_data, &val_p, Lux::IO::USER));
      ASSERT_EQ(correct.size(), val_p->size);
      ASSERT_TRUE(strncmp((char *) val_p->data, correct.c_str(), val_p->size) == 0);
    }

    ASSERT_EQ(true, bt->close()); 
  }

  /* 
   * operation: put, update
   * sequence: ordered
   * key: string
   * val: string
   * omode: create
   **/
  TEST_F(BtreeTest, UpdatePaddedTest) {
    bt->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[41];
      memset(key, 0, 41);
      sprintf(key, "%040d", i);
      // put
      ASSERT_EQ(true, bt->put(key, strlen(key), key, strlen(key)));
      std::string val = std::string(key) + std::string(key);
      // update
      ASSERT_EQ(true, bt->put(key, strlen(key), val.c_str(), val.size()));
    }

    ASSERT_EQ(true, bt->close()); 
  }

  /* 
   * operation: get
   * sequence: ordered
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, GetUpdatedPaddedTest) {
    bt->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[41];
      memset(key, 0, 41);
      sprintf(key, "%040d", i);
      std::string correct = std::string(key) + std::string(key);

      // sequential, system memory
      Lux::IO::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_EQ(correct.size(), val_data->size);
      ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
      bt->clean_data(val_data);

      // user memory
      char val[81];
      memset(val, 0, 81);
      Lux::IO::data_t key_data = {key, strlen(key)};
      Lux::IO::data_t val_data2 = {val, 0, 81}; // no NULL space
      Lux::IO::data_t *val_p = &val_data2;

      ASSERT_EQ(true, bt->get(&key_data, &val_p, Lux::IO::USER));
      ASSERT_EQ(correct.size(), val_p->size);
      ASSERT_TRUE(strcmp((char *) val_p->data, correct.c_str()) == 0);
    }

    ASSERT_EQ(true, bt->close()); 
  }

}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
