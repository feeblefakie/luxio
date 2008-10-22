#include "../array.h"
#include <gtest/gtest.h>

#define SMALL_NUM_ENTRIES 100000
#define DEFAULT_NUM_ENTRIES 1000000
#define LARGE_NUM_ENTRIES 10000000

namespace {
  
  static uint32_t db_num_ = 0;

  class ArrayTest : public testing::Test {
  protected:  
    virtual void SetUp()
    {
      db_name_ = "aryc_test";
      num_entries_ = SMALL_NUM_ENTRIES;
    }
    virtual void TearDown()
    {
      ary = NULL;
    }
    std::string get_db_name(uint32_t num)
    {
      char buf[256];
      memset(buf, 0, 256);
      sprintf(buf, "%d", num);
      std::string db_name = db_name_ + buf; 
      return db_name;
    }
   
    Lux::DBM::Array *ary;
    std::string db_name_;
    uint32_t num_entries_;
  };

  TEST_F(ArrayTest, PutTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::DBM::NOOVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      // sequential, system memory
      Lux::DBM::data_t *val_data = ary->get(i);
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      ary->clean_data(val_data);

      int val;
      Lux::DBM::data_t val_data2 = {&val, 0, sizeof(int)};
      Lux::DBM::data_t *val_p = &val_data2;
      ASSERT_EQ(true, ary->get(i, &val_p, Lux::DBM::USER));
      ASSERT_TRUE(i == val);
      ASSERT_TRUE(sizeof(int) == val_p->size);
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::DBM::NOOVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      // system memory
      Lux::DBM::data_t *val_data = ary->get(i);
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      ary->clean_data(val_data);

      // user memory
      int val;
      Lux::DBM::data_t val_data2 = {&val, 0, sizeof(int)};
      Lux::DBM::data_t *val_p = &val_data2;
      ASSERT_EQ(true, ary->get(i, &val_p, Lux::DBM::USER));
      ASSERT_TRUE(i == val);
      ASSERT_TRUE(sizeof(int) == val_p->size);
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::DBM::NOOVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    for (int i = 0; i < num_entries_; ++i) {
      // system memory
      Lux::DBM::data_t *val_data = ary->get(i);
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);
      ary->clean_data(val_data);

      // user memory
      int val;
      Lux::DBM::data_t val_data2 = {&val, 0, sizeof(int)};
      Lux::DBM::data_t *val_p = &val_data2;
      ASSERT_EQ(true, ary->get(i, &val_p, Lux::DBM::USER));
      ASSERT_TRUE(i == val);
      ASSERT_TRUE(sizeof(int) == val_p->size);
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }
}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
