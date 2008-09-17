#include <iostream>
#include <string.h>
#include <stdlib.h>

char *get_prefix_key(char *big, char *small);

int main(void)
{
  char *big = "electronical";
  char *small = "electronicam";
  //char *small = "elecom";

  char *prefix = get_prefix_key(big, small);
  std::cout << "prefix [" << prefix << "]" << std::endl;

  return 0;
}

char *get_prefix_key(char *big, char *small)
{
  size_t len = strlen(big) > strlen(small) ? strlen(small) : strlen(big);
  char *prefix = new char[len+2];
  memset(prefix, 0, len+2);

  int prefix_off = 0;
  for (int i = 0; i < len; ++i, ++prefix_off) {
    if (big[i] != small[i]) {
      break;
    }
  }
  memcpy(prefix, big, prefix_off+1);
  return prefix;
}
