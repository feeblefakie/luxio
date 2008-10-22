#include "../btree.h"
#include <gtest/gtest.h>

#define SMALL_NUM_ENTRIES 100000
#define DEFAULT_NUM_ENTRIES 1000000
#define LARGE_NUM_ENTRIES 10000000

namespace {
  
  static uint32_t db_num_ = 0;

  class BtreeTest : public testing::Test {
  protected:  
    virtual void SetUp()
    {
      bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
      db_name_ = "btc_test";
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
   
    Lux::DBM::Btree *bt; 
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
  TEST_F(BtreeTest, PutTest) {
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
                &i, sizeof(int), Lux::DBM::NOOVERWRITE));
      ASSERT_EQ(i+1, bt->get_auto_increment_id());
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
  TEST_F(BtreeTest, GetTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      // sequential, system memory
      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
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
      Lux::DBM::data_t key_data = {key, strlen(key)};
      Lux::DBM::data_t val_data2 = {&val, sizeof(int)};

      // sequntial, user memory
      ASSERT_EQ(true, bt->get(&key_data, &val_data2));
      ASSERT_TRUE(i == val);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: debug print
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   **/
  TEST_F(BtreeTest, DebugPrintTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    bt->show_db_header();
    bt->show_root();

    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: cursor
   * sequence: random
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, CursorTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    char buf[9] = "00001000";
    Lux::DBM::data_t key;
    int num = 1000;
    key.data = buf;
    key.size = 8;
    Lux::DBM::cursor_t *c = bt->cursor_init();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(true, bt->get(c, &key));
    while (bt->next(c)) {
      Lux::DBM::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM));
      ++num;
      ASSERT_EQ(num, *(int *) val->data);
    }
    num = 0;
    ASSERT_EQ(true, bt->first(c));
    do {
      Lux::DBM::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM));
      ASSERT_EQ(num, *(int *) val->data);
      ++num;
    } while (bt->next(c));
    num = num_entries_ - 1;
    ASSERT_EQ(true, bt->last(c));
    do {
      Lux::DBM::data_t *key, *val;
      ASSERT_EQ(true, bt->cursor_get(c, &key, &val, Lux::DBM::SYSTEM));
      ASSERT_EQ(num, *(int *) val->data);
      --num;
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
  TEST_F(BtreeTest, DelTest) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDWR));

    for (int i = 0; i < num_entries_; ++i) {
      if (i % 2 == 0) {
        continue;
      }
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      bt->clean_data(val_data);
      ASSERT_EQ(true, bt->del(key, strlen(key)));
      val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(bt->get(key, strlen(key)) == NULL);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put
   * sequence: random
   * key: string
   * val: binary
   * omode: create
   **/
  TEST_F(BtreeTest, PutTestRandom) {
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      ASSERT_EQ(true, bt->put(key, strlen(key), &num, sizeof(int)));
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: get
   * sequence: random
   * key: string
   * val: binary
   * omode: rdonly
   **/
  TEST_F(BtreeTest, GetTestRandom) {
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      if (val_data != NULL) {
        ASSERT_TRUE(num == *(int *) val_data->data);
        bt->clean_data(val_data);
      }
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put
   * sequence: ordered
   * key: int
   * val: string
   * omode: create
   **/
  TEST_F(BtreeTest, PutTestKeyBinary) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_cmp_func(Lux::DBM::int32_cmp_func);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char val[9];
      memset(val, 0, 9);
      sprintf(val, "%08d", i);

      ASSERT_EQ(true, bt->put(&i, sizeof(int), val, strlen(val)));
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: get
   * sequence: random
   * key: binary
   * val: string
   * omode: rdonly
   **/
  TEST_F(BtreeTest, GetTestKeyBinary) {
    std::string db_name = get_db_name(db_num_);
    bt->set_cmp_func(Lux::DBM::int32_cmp_func);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char val[9];
      memset(val, 0, 9);
      sprintf(val, "%08d", i);

      Lux::DBM::data_t *val_data = bt->get(&i, sizeof(int));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_EQ(0, strncmp((char *) val_data->data, val, val_data->size));
      bt->clean_data(val_data);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put, get
   * key: string
   * val: binary
   * omode: create
   **/
  TEST_F(BtreeTest, KeySizeLimitationTest) {
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    char key1[Lux::DBM::MAX_KSIZE+1];
    memset(key1, 0, Lux::DBM::MAX_KSIZE + 1);
    memset(key1, 'a', Lux::DBM::MAX_KSIZE);
    char key2[Lux::DBM::MAX_KSIZE+2];
    memset(key2, 0, Lux::DBM::MAX_KSIZE + 2);
    memset(key2, 'a', Lux::DBM::MAX_KSIZE + 1);
    int val = 10;

    ASSERT_EQ(true, bt->put(key1, strlen(key1), &val, sizeof(int)));
    Lux::DBM::data_t *val_data = bt->get(key1, strlen(key1));
    ASSERT_TRUE(val_data != NULL);
    bt->clean_data(val_data);
    ASSERT_EQ(false, bt->put(key2, strlen(key2), &val, sizeof(int)));
    ASSERT_TRUE(bt->get(key2, strlen(key2)) == NULL);

    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put, get
   * key: int
   * val: string
   * omode: create
   **/
  TEST_F(BtreeTest, ValueSizeLimitationTest) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_cmp_func(Lux::DBM::int32_cmp_func);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    int key = 10;
    char val1[Lux::DBM::CLUSTER_MAX_VSIZE+1];
    memset(val1, 0, Lux::DBM::CLUSTER_MAX_VSIZE + 1);
    memset(val1, 'a', Lux::DBM::CLUSTER_MAX_VSIZE);
    char val2[Lux::DBM::CLUSTER_MAX_VSIZE+2];
    memset(val2, 0, Lux::DBM::CLUSTER_MAX_VSIZE + 2);
    memset(val2, 'a', Lux::DBM::CLUSTER_MAX_VSIZE + 1);

    ASSERT_EQ(false, bt->put(&key, sizeof(int), val2, strlen(val2)));
    Lux::DBM::data_t *val_data = bt->get(&key, sizeof(int));
    ASSERT_TRUE(val_data == NULL);
    ASSERT_EQ(true, bt->put(&key, sizeof(int), val1, strlen(val1)));
    val_data = bt->get(&key, sizeof(int));
    ASSERT_TRUE(bt->get(&key, sizeof(int)) != NULL);
    bt->clean_data(val_data);

    ASSERT_EQ(true, bt->close());
  }

  /* 
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   * concurrency: process
   **/
  TEST_F(BtreeTest, ProcessTest) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_lock_type(Lux::DBM::LOCK_PROCESS);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      ASSERT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));
    }
    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      bt->clean_data(val_data);
    }

    ASSERT_EQ(true, bt->close());
  }

  /* 
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   * concurrency: thread
   **/
  TEST_F(BtreeTest, ThreadTest) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_lock_type(Lux::DBM::LOCK_THREAD);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      ASSERT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));
    }
    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      bt->clean_data(val_data);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   * pagesize: 1024
   **/
  TEST_F(BtreeTest, PutTestPageSizeSmallest) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_page_size(1024);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      ASSERT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));
    }
    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      bt->clean_data(val_data);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   * pagesize: 65536
   **/
  TEST_F(BtreeTest, PutTestPageSizeBiggest) {
    std::string db_name = get_db_name(++db_num_);
    bt->set_page_size(65536);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      ASSERT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));

    }
    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      bt->clean_data(val_data);
    }
    ASSERT_EQ(true, bt->close());
  }

  /* 
   * operation: put with bulk loading
   * sequence: ordered
   * key: string
   * val: binary
   * omode: create
   **/
  TEST_F(BtreeTest, PutTestWithBulkLoading) {
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, bt->open(db_name.c_str(), Lux::DB_CREAT));

    bt->set_bulk_loading(true);
    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      ASSERT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));
    }
    bt->set_bulk_loading(false);

    ASSERT_EQ(true, bt->close());
  }
}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
