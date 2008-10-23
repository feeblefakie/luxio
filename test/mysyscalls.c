#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdlib.h>

/*
 * these are functions used by test programs
 */

/*
 * write
 */
static ssize_t (*write0)(int fd, const void *buf, size_t count);
void __attribute__((constructor))
init_write0()
{
  write0 = dlsym(RTLD_NEXT, "write");
}

ssize_t write(int fd, const void *buf, size_t count)
{
  char *p = getenv("SIM_WRITE");
  if (p == NULL) {
    return (*write0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] write failed\n");
  return -1;
}

/**
 * lseek
 */
static off_t (*lseek0)(int fildes, off_t offset, int whence)
void __attribute__((constructor))
init_lseek0()
{
  lseek0 = dlsym(RTLD_NEXT, "lseek");
}

off_t lseek(int fildes, off_t offset, int whence)
{
  char *p = getenv("SIM_LSEEK");
  if (p == NULL) {
    return (*lseek0)(fildes, offset, whence);
  }
  fprintf(stderr, "[simulation] lseek failed\n");
  return -1;
}

/*
 * read
 */
static ssize_t (*read0)(int fd, void *buf, size_t count)
void __attribute__((constructor))
init_read0()
{
  read0 = dlsym(RTLD_NEXT, "read");
}

ssize_t read(int fd, void *buf, size_t count)
{
  char *p = getenv("SIM_READ");
  if (p == NULL) {
    return (*read0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] read failed\n");
  return -1;
}

/*
 * mmap
 */
static void *(*mmap0)(void *start, size_t length, int prot, int flags, int fd, off_t offset)
void __attribute__((constructor))
init_mmap0()
{
  mmap0 = dlsym(RTLD_NEXT, "mmap");
}

void *mmap(void *start, size_t length, int prot, int flags, int fd, off_t offset)
{
  char *p = getenv("SIM_MMAP");
  if (p == NULL) {
    return (*mmap0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] mmap failed\n");
  return MAP_FAILED;
}

/*
 * munmap
 */
static int (*munmap0)(void *start, size_t length)
void __attribute__((constructor))
init_munmap0()
{
  munmap0 = dlsym(RTLD_NEXT, "munmap");
}

int munmap(void *start, size_t length)
{
  char *p = getenv("SIM_MUNMAP");
  if (p == NULL) {
    return (*munmap0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] munmap failed\n");
  return -1;
}

/*
 * ftruncate
 */
static int (*ftruncate0)ftruncate(int fd, off_t length)
void __attribute__((constructor))
init_ftruncate0()
{
  ftruncate0 = dlsym(RTLD_NEXT, "ftruncate");
}

int ftruncate(int fd, off_t length)
{
  char *p = getenv("SIM_FTRUNCATE");
  if (p == NULL) {
    return (*ftruncate0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] ftruncate failed\n");
  return -1;
}
