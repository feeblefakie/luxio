#include "../btree.h"
#include <gtest/gtest.h>

namespace {

  class BtreeTest : public testing::Test {
  protected:  
    virtual void SetUp() {
      bt = new Lux::DBM::Btree(Lux::DBM::CLUSTER);
      //bt->set_lock_type(Lux::DBM::LOCK_PROCESS);
      //bt->set_page_size(4096);
      std::cout << "new" << std::endl;
    }
    virtual void TearDown() {
      delete bt;
      std::cout << "delete" << std::endl;
    }
   
    Lux::DBM::Btree *bt; 
  };

  TEST_F(BtreeTest, PutTest) {
    ASSERT_EQ(true, bt->open("benchdb", Lux::DB_CREAT));

    for (int i = 0; i < 100000; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);

      EXPECT_EQ(true, bt->put(key, strlen(key), &i, sizeof(uint32_t)));

    }
    ASSERT_EQ(true, bt->close());
  }

  TEST_F(BtreeTest, GetTest) {
    ASSERT_EQ(true, bt->open("benchdb", Lux::DB_RDONLY));

    for (int i = 0; i < 100000; ++i) {
      char key[9];
      memset(key, 0, 9);
      sprintf(key,"%08d", i);

      Lux::DBM::data_t *val_data = bt->get(key, strlen(key));
      ASSERT_TRUE(val_data != NULL);
      ASSERT_TRUE(i == *(int *) val_data->data);

    }
    ASSERT_EQ(true, bt->close());
  }

}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}
