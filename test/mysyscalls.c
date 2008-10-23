#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>

/*
 * these are functions used by test programs
 */

#ifdef DEBUG_WRITE
ssize_t write(int fd, const void *buf, size_t count)
{
  fprintf(stderr, "[simulation] write failed\n");
  return -1;
}
#endif

#ifdef DEBUG_LSEEK
off_t lseek(int fildes, off_t offset, int whence)
{
  fprintf(stderr, "[simulation] lseek failed\n");
  return -1;
}
#endif

#ifdef DEBUG_READ
ssize_t read(int fd, void *buf, size_t count)
{
  fprintf(stderr, "[simulation] read failed\n");
  return -1;
}
#endif

#ifdef DEBUG_MMAP
void *mmap(void *start, size_t length, int prot, int flags, int fd, off_t offset)
{
  fprintf(stderr, "[simulation] mmap failed\n");
  return MAP_FAILED;
}
#endif

#ifdef DEBUG_MUNMAP
int munmap(void *start, size_t length)
{
  fprintf(stderr, "[simulation] munmap failed\n");
  return -1;
}
#endif

#ifdef DEBUG_FTRUNCATE
int ftruncate(int fd, off_t length)
{
  fprintf(stderr, "[simulation] ftruncate failed\n");
  return -1;
}
#endif
