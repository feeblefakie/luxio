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

    void GetClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        // sequential, system memory
        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_TRUE(i == *(int *) val_data->data);
        ary->clean_data(val_data);

        int val;
        Lux::IO::data_t val_data2 = {&val, 0, sizeof(int)};
        Lux::IO::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::IO::USER));
        ASSERT_TRUE(i == val);
        ASSERT_TRUE(sizeof(int) == val_p->size);

        Lux::IO::data_t *val_data3;
        ASSERT_EQ(true, ary->get(i, &val_data3, Lux::IO::SYSTEM));
        ASSERT_TRUE(val_data3 != NULL);
        ASSERT_TRUE(i == *(int *) val_data3->data);
        ary->clean_data(val_data3);
      }
    }

    void DelClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        if (i % 2 == 0) {
          continue;
        }

        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ary->clean_data(val_data);
        ASSERT_EQ(true, ary->del(i));
      }
    }

    void PutNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        // put
        ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
        // put
        ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::IO::NOOVERWRITE));
      }
    }

    void GetNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        // system memory
        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_TRUE(i == *(int *) val_data->data);
        ary->clean_data(val_data);

        // user memory
        int val;
        Lux::IO::data_t val_data2 = {&val, 0, sizeof(int)};
        Lux::IO::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::IO::USER));
        ASSERT_TRUE(i == val);
        ASSERT_TRUE(sizeof(int) == val_p->size);
      }
    }

    void UpdateNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        // put
        ASSERT_EQ(true, ary->put(i, val, strlen(val)));
        // update
        std::string val2 = std::string(val) + std::string(val);
        ASSERT_EQ(true, ary->put(i, val2.c_str(), val2.size(), Lux::IO::OVERWRITE));
      }
    }

    void GetUpdateNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        std::string correct = std::string(val) + std::string(val);

        // system memory
        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_EQ(correct.size(), val_data->size);
        ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
        ary->clean_data(val_data);

        // user memory
        char val2[80];
        memset(val2, 0, 80);
        Lux::IO::data_t val_data2 = {val2, 0, 80};
        Lux::IO::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::IO::USER));
        ASSERT_EQ(correct.size(), val_p->size);
        ASSERT_TRUE(strncmp((char *) val_p->data, correct.c_str(), val_p->size) == 0);
      }
    }

    void AppendNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        // put
        ASSERT_EQ(true, ary->put(i, val, strlen(val)));
        // update
        ASSERT_EQ(true, ary->put(i, val, strlen(val), Lux::IO::APPEND));
      }
    }

    void GetAppendNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        char val[41];
        memset(val, 0, 41);
        sprintf(val, "%040d", i);
        std::string correct = std::string(val) + std::string(val);

        // system memory
        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ASSERT_EQ(correct.size(), val_data->size);
        ASSERT_TRUE(strncmp((char *) val_data->data, correct.c_str(), val_data->size) == 0);
        ary->clean_data(val_data);

        // user memory
        char val2[80];
        memset(val2, 0, 80);
        Lux::IO::data_t val_data2 = {val2, 0, 80};
        Lux::IO::data_t *val_p = &val_data2;
        ASSERT_EQ(true, ary->get(i, &val_p, Lux::IO::USER));
        ASSERT_EQ(correct.size(), val_p->size);
        ASSERT_TRUE(strncmp((char *) val_p->data, correct.c_str(), val_p->size) == 0);
      }
    }

    void DelNonClusterTest(Lux::IO::Array *ary)
    {
      for (int i = 0; i < num_entries_; ++i) {
        if (i % 2 == 0) {
          continue;
        }

        Lux::IO::data_t *val_data = ary->get(i);
        ASSERT_TRUE(val_data != NULL);
        ary->clean_data(val_data);
        ASSERT_EQ(true, ary->del(i));
      }
    }

    Lux::IO::Array *ary;
    std::string db_name_;
    uint32_t num_entries_;
  };

  TEST_F(ArrayTest, PutTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::IO::NOOVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int), Lux::IO::OVERWRITE));
    }
    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DebugPrintTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

#ifdef DEBUG
    ary->show_db_header();
#endif

    ASSERT_EQ(true, ary->close());
    delete ary;
  }


  TEST_F(ArrayTest, DelTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDWR));

    DelClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    PutNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DelNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    DelNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    UpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetUpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, AppendNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    AppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetAppendNonClusterPaddedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Padded);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetAppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, PutNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    PutNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, DelNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    DelNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, UpdateNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    UpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetUpdateNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetUpdateNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, AppendNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    AppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, GetAppendNonClusterLinkedTest) {
    ary = new Lux::IO::Array(Lux::IO::NONCLUSTER);
    ary->set_noncluster_params(Lux::IO::Linked);
    std::string db_name = get_db_name(db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_RDONLY));

    GetAppendNonClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, ProcessTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    ary->set_lock_type(Lux::IO::LOCK_PROCESS);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
    }
    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }

  TEST_F(ArrayTest, ThreadTest) {
    ary = new Lux::IO::Array(Lux::IO::CLUSTER);
    ary->set_lock_type(Lux::IO::LOCK_THREAD);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, ary->open(db_name.c_str(), Lux::IO::DB_CREAT));

    for (int i = 0; i < num_entries_; ++i) {
      // put
      ASSERT_EQ(true, ary->put(i, &i, sizeof(int)));
    }
    GetClusterTest(ary);

    ASSERT_EQ(true, ary->close());
    delete ary;
  }
}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
