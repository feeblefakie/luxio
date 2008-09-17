#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int main(void)
{
  int fd = open("maptest", O_RDWR, 00644);
  if (fd < 0) {
    std::cerr << "open failed" << std::endl;
    return -1;
  }

  int size = 1024;

  std::cout << "mapping ..." << std::endl;
  int *map = (int *) mmap(0, size * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
  if (map == MAP_FAILED) {
    std::cerr << "map failed" << std::endl;
    return -1;
  }

  while (1) {
    std::cout << map[0] << std::endl;
    usleep(50000);
  }

  close(fd);
}
