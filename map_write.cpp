#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int main(void)
{
  int fd = open("maptest", O_CREAT | O_RDWR, 00644);
  if (fd < 0) {
    std::cerr << "open failed" << std::endl;
    return -1;
  }

  int chunk = 1024;
  int size = chunk;

  while (1) {
    if (ftruncate(fd, size * sizeof(int)) < 0) {
      std::cerr << "ftruncate failed" << std::endl;
      return -1;
    }
    std::cout << "mapping " << size << " ..." << std::endl;
    int *map = (int *) mmap(0, size * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
    if (map == MAP_FAILED) {
      std::cerr << "map failed" << std::endl;
      return -1;
    }
    for (int i = 0; i < size; ++i) {
      map[i] = i+size;
    }
    sleep(3);
    munmap(map, size);
    size += chunk;
  }

  close(fd);
}
