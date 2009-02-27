#include <iostream>

void func(off_t num);
off_t get(void);

int main(void)
{
  uint32_t a = 1024;
  uint32_t b = 1024*1024*4;
  func((off_t) a*b);

  return 0;
} 

void func(off_t num)
{
  std::cout << "func: " << num << std::endl;
}

off_t get(void)
{
    
}
