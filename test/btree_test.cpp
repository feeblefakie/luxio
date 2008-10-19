#include "../btree.h"
#include <gtest/gtest.h>

#define SMALL_NUM_ENTRIES 100000
#define DEFAULT_NUM_ENTRIES 1000000
#define LARGE_NUM_ENTRIES 10000000

namespace {

  class BtreeTest : public testing::Test {
  protected:  
    virtual void SetUp() {
      bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
      db_name_ = "bttest";
      num_entries_ = SMALL_NUM_ENTRIES;
    }
    virtual void TearDown() {
      delete bt;
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
  TEST_F(BtreeTest, PutTest1) {
    std::string db_name = db_name_ + "_1";
    ASSERT_EQ(true, bt->open(db_name_.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);
      EXPECT_EQ(true, bt->put(key, strlen(key), &i, sizeof(int)));
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
  TEST_F(BtreeTest, GetTest1_1) {
    std::string db_name = db_name_ + "_1";
    ASSERT_EQ(true, bt->open(db_name_.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key, "%08d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
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
  TEST_F(BtreeTest, GetTest1_2) {
    std::string db_name = db_name_ + "_1";
    ASSERT_EQ(true, bt->open(db_name_.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(num == *(int *) val_data->data);
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
  TEST_F(BtreeTest, PutTest2) {
    std::string db_name = db_name_ + "_2";
    ASSERT_EQ(true, bt->open(db_name_.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      EXPECT_EQ(true, bt->put(key, strlen(key), &num, sizeof(int)));
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
  TEST_F(BtreeTest, GetTest2) {
    std::string db_name = db_name_ + "_2";
    ASSERT_EQ(true, bt->open(db_name_.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      char key[9];
      memset(key, 0, 9);
      int num = rand() % num_entries_;
      sprintf(key, "%08d", num);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      if (val_data != NULL) {
        ASSERT_TRUE(num == *(int *) val_data->data);
      }
    }
    ASSERT_EQ(true, bt->close());
  }

}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
