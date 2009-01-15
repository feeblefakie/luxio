#ifndef LUX_UTIL_H
#define LUX_UTIL_H

#define _XOPEN_SOURCE 50

#include "types.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <string>
#include <iostream>
#include <unistd.h>

#ifdef DEBUG
#define error_log(msg) \
  std::cerr << "[error] " << msg \
            << " in " << __FILE__ << ":" << __LINE__ \
            << std::endl; 
#else
#define error_log(msg)
#endif

// for verbose info logging
#ifdef DEBUG_VINFO
#define vinfo_log(msg) \
  std::cerr << "[info] " << msg \
            << std::endl; 
#else
#define vinfo_log(msg)
#endif

// System call wrapper 
// used in gauche. (macro names are changed.)
// thanks to shirok and kzk.
// http://kzk9.net/column/write.html
#define SAFE_SYSCALL3(result, expr, check) \
  do { \
    (result) = (expr); \
    if ((check) && errno == EINTR) { \
      errno = 0; \
    } else { \
      break; \
    } \
  } while (1)

#define SAFE_SYSCALL(result, expr) \
  SAFE_SYSCALL3(result, expr, (result < 0))

namespace Lux {

  static inline void _mkdir(const char *str)
  {
    std::string str_(str); 
    int n = -1; 
    while (1) {
      n = str_.find_first_of('/', n+1);
      if (n == std::string::npos) {
        break;  
      }
      std::string dir = str_.substr(0, n);
      // [TODO] error handling
      ::mkdir(dir.c_str(), 0755);
    }
  }

  static inline int _open(const char *pathname, int flags, mode_t mode)
  {
    int oflags = O_RDONLY;

    if (flags & DB_RDWR) {
      oflags |= O_RDWR;
    }
    if (flags & DB_CREAT) {
      oflags |= O_CREAT | O_RDWR;
      _mkdir(pathname);
    }
    if (flags & DB_TRUNC) {
      oflags |= O_TRUNC;
    }
    return ::open(pathname, oflags, mode);
  }
  
  static inline ssize_t _read(int fd, void *buf, size_t count)
  {
    char *p = reinterpret_cast<char *>(buf);
    const char * const end_p = p + count;

    while (p < end_p) {
      int num_bytes;
      SAFE_SYSCALL(num_bytes, read(fd, p, end_p - p));
      if (num_bytes < 0) {
        perror("read failed");
        break;
      }
      p += num_bytes;
    }

    if (p != end_p) {
      return -1;
    }
    return count;
  }
  /*
  static inline ssize_t _read(int fd, void *buf, size_t count)
  {
    char *p = reinterpret_cast<char *>(buf);
    const char * const end_p = p + count;

    while (p < end_p) {
      const int num_bytes = read(fd, p, end_p - p);
      if (num_bytes < 0) {
        if (errno == EINTR) {
          continue;
        }
        perror("read failed");
        break;
      }
      p += num_bytes;
    }

    if (p != end_p) {
      return -1;
    }
    return count;
  }
  */

  static inline ssize_t _write(int fd, const void *buf, size_t count)
  {
    const char *p = reinterpret_cast<const char *>(buf);
    const char * const end_p = p + count;

    while (p < end_p) {
      int num_bytes;
      SAFE_SYSCALL(num_bytes, write(fd, p, end_p - p));
      if (num_bytes < 0) {
        perror("write failed");
        break;
      }
      p += num_bytes;
    }

    if (p != end_p) {
      return -1;
    }
    return count;
  }
  /*
  static inline ssize_t _write(int fd, const void *buf, size_t count)
  {
    const char *p = reinterpret_cast<const char *>(buf);
    const char * const end_p = p + count;

    while (p < end_p) {
      const int num_bytes = write(fd, p, end_p - p);
      if (num_bytes < 0) {
        if (errno == EINTR) continue;
        perror("write failed");
        break;
      }
      p += num_bytes;
    }

    if (p != end_p) {
      return -1;
    }
    return count;
  }
  */

  static inline bool _pwrite(int fd, const void *buf, size_t nbyte, off_t offset)
  {
    const char *p = reinterpret_cast<const char *>(buf);
    const char * const end_p = p + nbyte;

    while (p < end_p) {
      int num_bytes;
      SAFE_SYSCALL(num_bytes, pwrite(fd, p, end_p - p, offset));
      if (num_bytes < 0) {
        perror("write failed");
        break;
      }
      p += num_bytes;
      offset += num_bytes;
    }

    if (p != end_p) {
      return false;
    }
    return true;
  }
  /*
  static inline bool _pwrite(int fd, const void *buf, size_t nbyte, off_t offset)
  {
    if (pwrite(fd, buf, nbyte, offset) < 0) {
      perror("pwrite failed");
      return false;
    }
    return true;
  }
*/

  static inline bool _pread(int fd, void *buf, size_t nbyte, off_t offset)
  {
    char *p = reinterpret_cast<char *>(buf);
    const char * const end_p = p + nbyte;

    while (p < end_p) {
      int num_bytes;
      SAFE_SYSCALL(num_bytes, pread(fd, p, end_p - p, offset));
      if (num_bytes < 0) {
        perror("read failed");
        break;
      }
      p += num_bytes;
      offset += num_bytes;
    }

    if (p != end_p) {
      return false;
    }
    return true;
  }
  /*
  static inline bool _pread(int fd, void *buf, size_t nbyte, off_t offset)
  {
    if (pread(fd, buf, nbyte, offset) < 0) {
      perror("pread failed");
      return false;
    }
    return true;
  }
  */

  static inline void *_mmap(int fd, size_t size, int flags)
  {
    int prot = PROT_READ;
    if (flags & DB_RDWR || flags & DB_CREAT) {
      prot |= PROT_WRITE;
    }

    void *p = mmap(0, size, prot, MAP_SHARED, fd, 0);  
    if (p == MAP_FAILED) {
      return NULL;
    }
    return p;
  }

}

#endif
