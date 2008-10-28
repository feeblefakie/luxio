#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " num" << std::endl; 
    exit(1);
  }

  int num = atoi(argv[1]);

  for (int i = 0; i < num; ++i) {
    char key[9];
    memset(key, 0, 9);
    sprintf(key,"%08d", i);
    std::cout << rand() % num << " " << key << " " << i << std::endl;
  }

  return 0;
}
