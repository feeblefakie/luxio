#include "../data.h"
#include <gtest/gtest.h> 
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>

#define APPEND_LINES 10

namespace {
  
  static uint32_t db_num_ = 0;

  class DataTest : public testing::Test {
  protected:  
    virtual void SetUp()
    {
      db_name_ = "data_test";
    }
    virtual void TearDown()
    {
      delete dt;
      dt = NULL;
    }
    std::string get_db_name(uint32_t num)
    {
      char buf[256];
      memset(buf, 0, 256);
      sprintf(buf, "%d", num);
      std::string db_name = db_name_ + buf + ".data";
      return db_name;
    }
   
    void data_test(Lux::DBM::Data *dt)
    {
      std::ifstream fin;
      fin.open("./data.txt", std::ios::in);
      if (!fin) {
        std::cout << "cannot open the file" << std::endl;
        exit(-1);
      }

      std::vector<Lux::DBM::data_ptr_t *> vec;
      std::vector<std::string> chunks;
      Lux::DBM::data_ptr_t *data_ptr;
      std::string line;
      std::string chunk;
      int i = 0;
      while (getline(fin, line)) {
        Lux::DBM::data_t data = {line.c_str(), line.size()};
        if (i == 0) {
          data_ptr = dt->put(&data);
        } else if (i % APPEND_LINES == 0) {
          if ((random() % 3) == 2) {
            dt->del(data_ptr);
          } else {
            vec.push_back(data_ptr);
            chunks.push_back(chunk);
          }
          chunk.clear();
          data_ptr = dt->put(&data);
        } else {
          data_ptr = dt->append(data_ptr, &data);
        }
        chunk += line;
        ++i;
      }
      fin.close();
     
      std::vector<Lux::DBM::data_ptr_t *>::iterator itr = vec.begin();
      std::vector<Lux::DBM::data_ptr_t *>::iterator itr_end = vec.end();

      i = 0;
      while (itr != itr_end) {
        Lux::DBM::data_t *data = dt->get(*itr);
        ASSERT_EQ(chunks[i].size(), data->size);
        ASSERT_TRUE(strncmp(chunks[i].c_str(), (char *) data->data, data->size) == 0);
        dt->clean_data(data);
        dt->clean_data_ptr(*itr);
        ++itr;
        ++i;
      }
    }

    Lux::DBM::Data *dt; 
    std::string db_name_;

  };

  TEST_F(DataTest, PaddedDataTest) {
    dt = new Lux::DBM::PaddedData();
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);
    //dt->show_db_header();
    //dt->show_free_pools();

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, NoPaddedDataTest) {
    dt = new Lux::DBM::PaddedData();
    dt->set_padding(Lux::DBM::NOPADDING);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, FixedPaddedDataTest) {
    dt = new Lux::DBM::PaddedData();
    dt->set_padding(Lux::DBM::FIXEDLEN);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, RatioPaddedDataTest) {
    dt = new Lux::DBM::PaddedData();
    dt->set_padding(Lux::DBM::RATIO);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, LinkedDataTest) {
    dt = new Lux::DBM::LinkedData();
    dt->set_padding(Lux::DBM::PO2);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);
    //dt->show_db_header();
    //dt->show_free_pools();

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, NoLinkedDataTest) {
    dt = new Lux::DBM::LinkedData();
    dt->set_padding(Lux::DBM::NOPADDING);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, FixedLinkedDataTest) {
    dt = new Lux::DBM::LinkedData();
    dt->set_padding(Lux::DBM::FIXEDLEN);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

  TEST_F(DataTest, RatioLinkedDataTest) {
    dt = new Lux::DBM::LinkedData();
    dt->set_padding(Lux::DBM::RATIO);
    std::string db_name = get_db_name(++db_num_);
    ASSERT_EQ(true, dt->open(db_name.c_str(), Lux::DB_CREAT));

    data_test(dt);

    ASSERT_EQ(true, dt->close());
  }

}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
