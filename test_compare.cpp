#include <iostream>
#include <functional>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

double gettimeofday_sec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + (double)tv.tv_usec*1e-6;
}

#define ALLOC_AND_COPY(s1, s2, size) \
  char s1[size+1]; \
  memcpy(s1, s2, size); \
  s1[size] = '\0';

typedef struct {
  const void *data;
  uint32_t size;
} data_t;

int str_cmp_func(data_t &d1, data_t &d2)
{
  return strcmp((char *) d1.data, (char *) d2.data);
}

struct str_cmp_functor : public std::binary_function<data_t, data_t, int>
{
  int operator()(data_t &d1, data_t &d2) {
    return strcmp((char *) d1.data, (char *) d2.data);
  }
};

struct int32_cmp_functor : public std::binary_function<data_t, data_t, int>
{
  int operator()(data_t &d1, data_t &d2) {
    int32_t i1, i2;
    memcpy(&i1, d1.data, sizeof(int32_t));
    memcpy(&i2, d2.data, sizeof(int32_t));
    return (i1 - i2); 
  }
};

template <class cmp>
int compare_all(cmp cmp_functor)
{
  char *str1 = "10000000";
  data_t d1 = {str1, strlen(str1)};

  int i = 0;
  while (1) {
    char str2[9];
    memset(str2, 0, 9);
    sprintf(str2, "%08d", i);
    data_t d2 = {str2, strlen(str2)};

    //if (cmp_functor(d1, d2) < 0) {
    if (strcmp(str1, str2) < 0) {
      break;
    }
    ++i;
  }
  return 0;
}

int main(void)
{
  double t1, t2;
  t1 = gettimeofday_sec();
  //compare_all(str_cmp_functor());
  compare_all(str_cmp_func);
  t2 = gettimeofday_sec();
  std::cout << "compare time: " << t2 - t1 << std::endl;

  return 0;
}

