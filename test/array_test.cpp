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

    void GetClusterTest(Lux::DBM::Array *ary)
    {
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

        Lux::DBM::data_t *val_data3;
        ASSERT_EQ(true, ary->get(i, &val_data3, Lux::DBM::SYSTEM));
        ASSERT_TRUE(val_data3 != NULL);
        ASSERT_TRUE(i == *(int *) val_data3->data);
        ary->clean_data(val_data3);
      }
    }

    void DelClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        if (i % 2 == 0) {
          continue;
        }

        Lux::DBM::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ary->clean_data(val_data);
        ASSERT_EQ(true, ary->del(i));
      }
    }

    void PutNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        // put
        ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
        // put
        ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::DBM::NOOVERWRITE));
      }
    }

    void GetNonClusterTest(Lux::DBM::Array *ary)
    {
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
    }

    void UpdateNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        // put
        ASSERT_EQ(true, ary->put(i, val, strlen(val)));
        // update
        std::string val2 = std::string(val) + std::string(val);
        ASSERT_EQ(true, ary->put(i, val2.c_str(), val2.size(), Lux::DBM::OVERWRITE));
      }
    }

    void GetUpdateNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        std::string correct = std::string(val) + std::string(val);

        // system memory
        Lux::DBM::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_EQ(correct.size(), val_data->size);
        ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
        ary->clean_data(val_data);

        // user memory
        char val2[80];
        memset(val2, 0, 80);
        Lux::DBM::data_t val_data2 = {val2, 0, 80};
        Lux::DBM::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::DBM::USER));
        ASSERT_EQ(correct.size(), val_p->size);
        ASSERT_TRUE(strncmp((char *) val_p->data, correct.c_str(), val_p->size) == 0);
      }
    }

    void AppendNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        // put
        ASSERT_EQ(true, ary->put(i, val, strlen(val)));
        // update
        ASSERT_EQ(true, ary->put(i, val, strlen(val), Lux::DBM::APPEND));
      }
    }

    void GetAppendNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        std::string correct = std::string(val) + std::string(val);

        // system memory
        Lux::DBM::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_EQ(correct.size(), val_data->size);
        ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
        ary->clean_data(val_data);

        // user memory
        char val2[80];
        memset(val2, 0, 80);
        Lux::DBM::data_t val_data2 = {val2, 0, 80};
        Lux::DBM::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::DBM::USER));
        ASSERT_EQ(correct.size(), val_p->size);
        ASSERT_TRUE(strncmp((char *) val_p->data, correct.c_str(), val_p->size) == 0);
      }
    }

    void DelNonClusterTest(Lux::DBM::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        if (i % 2 == 0) {
          continue;
        }

        Lux::DBM::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ary->clean_data(val_data);
        ASSERT_EQ(true, ary->del(i));
      }
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

    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::DBM::OVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DelTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDWR));

    DelClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    PutNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DelNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    DelNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    UpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetUpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, AppendNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    AppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetAppendNonClusterPaddedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetAppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    PutNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DelNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    DelNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    UpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetUpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, AppendNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    AppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetAppendNonClusterLinkedTest) {
    ary = new Lux::DBM::Array(Lux::DBM::NONCLUSTER);
    ary->set_noncluster_params(Lux::DBM::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_RDONLY));

    GetAppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, ProcessTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    ary->set_lock_type(Lux::DBM::LOCK_PROCESS);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, ThreadTest) {
    ary = new Lux::DBM::Array(Lux::DBM::CLUSTER);
    ary->set_lock_type(Lux::DBM::LOCK_THREAD);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
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
