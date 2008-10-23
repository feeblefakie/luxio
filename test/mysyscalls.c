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
  int n;
  printf("exec libc write? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*write0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] write failed\n");
  return -1;
}

/**
 * lseek
 */
static off_t (*lseek0)(int fildes, off_t offset, int whence);
void __attribute__((constructor))
init_lseek0()
{
  lseek0 = dlsym(RTLD_NEXT, "lseek");
}

off_t lseek(int fildes, off_t offset, int whence)
{
  int n;
  printf("exec libc lseek? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*lseek0)(fildes, offset, whence);
  }
  fprintf(stderr, "[simulation] lseek failed\n");
  return -1;
}

/*
 * read
 */
static ssize_t (*read0)(int fd, void *buf, size_t count);
void __attribute__((constructor))
init_read0()
{
  read0 = dlsym(RTLD_NEXT, "read");
}

ssize_t read(int fd, void *buf, size_t count)
{
  int n;
  printf("exec libc read? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*read0)(fd, buf, count);
  }
  fprintf(stderr, "[simulation] read failed\n");
  return -1;
}

/*
 * mmap
 */
static void *(*mmap0)(void *start, size_t length, int prot, int flags, int fd, off_t offset);
void __attribute__((constructor))
init_mmap0()
{
  mmap0 = dlsym(RTLD_NEXT, "mmap");
}

void *mmap(void *start, size_t length, int prot, int flags, int fd, off_t offset)
{
  int n;
  printf("exec libc mmap? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*mmap0)(start, length, prot, flags, fd, offset);
  }
  fprintf(stderr, "[simulation] mmap failed\n");
  return MAP_FAILED;
}

/*
 * munmap
 */
static int (*munmap0)(void *start, size_t length);
void __attribute__((constructor))
init_munmap0()
{
  munmap0 = dlsym(RTLD_NEXT, "munmap");
}

int munmap(void *start, size_t length)
{
  int n;
  printf("exec libc munmap? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*munmap0)(start, length);
  }
  fprintf(stderr, "[simulation] munmap failed\n");
  return -1;
}

/*
 * ftruncate
 */
static int (*ftruncate0)(int fd, off_t length);
void __attribute__((constructor))
init_ftruncate0()
{
  ftruncate0 = dlsym(RTLD_NEXT, "ftruncate");
}

int ftruncate(int fd, off_t length)
{
  int n;
  printf("exec libc ftruncate? [1/2] ... \n");
  scanf("%d" , &n);
  if (n == 1) {
    return (*ftruncate0)(fd, length);
  }
  fprintf(stderr, "[simulation] ftruncate failed\n");
  return -1;
}
