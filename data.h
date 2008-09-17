/*
 * Copyright (C) 2008 Hiroyuki Yamada
 *  
 * This program is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published 
 * by the Free Software Foundation; version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#ifndef LIBMAP_DATA_H
#define LIBMAP_DATA__H

#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <stdexcept>

namespace LibMap {

  const int DEFAULT_PAGESIZE = getpagesize();
  typedef int db_flags_t;
  const db_flags_t DB_RDONLY = 0x0000;
  const db_flags_t DB_RDWR = 0x0002;
  const db_flags_t DB_CREAT = 0x0200;
  const db_flags_t DB_TRUNC = 0x0400;

// these should move to a shared header file
#pragma pack(2)
  typedef struct {
    uint32_t id;
    uint16_t off; // uint8_t でOK?
  } data_ptr_t;
#pragma pack()

  typedef struct {
    const void *data;
    uint32_t size;
  } data_t;
// these should move to a shared header file

  typedef uint32_t block_id_t;

  typedef struct {
    block_id_t id;
    uint16_t off;
  } free_pool_t;

  typedef struct {
    uint32_t num_blocks;
    uint32_t block_size;
    uint32_t bytes_used;
    free_pool_t first_free_ptr[32]; // ptr to first free space having 2^0 - 2^32 bytes (2^0 - 2^4 unused)
    free_pool_t last_free_ptr[32];
    off_t data_off; // points to area being allocated next
  } db_header_t;

  typedef struct {
    uint32_t size;
    block_id_t next_block_id;
  } record_header_t;

  typedef struct {
    record_header_t *h;
    data_t *d; 
    uint32_t size_ceiled;
  } record_t;

  typedef struct {
    block_id_t next_block_id;
  } freearea_header_t;

  /*
   * Class Data
   */
  class Data {
  public:
    Data()
    {}

    ~Data()
    {}

    bool open(std::string db_name, db_flags_t oflags)
    {
      fd_ = _open(db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd_, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        return false;
      }

      db_header_t dh;
      memset(&dh, 0, sizeof(db_header_t));
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        dh.num_blocks = 0;
        dh.block_size = getpagesize();
        dh.bytes_used = 0;
        for (int i = 0; i < 32; ++i) {
          dh.first_free_ptr[i].id = 0;
          dh.first_free_ptr[i].off = 0;
          dh.last_free_ptr[i].id = 0;
          dh.last_free_ptr[i].off = 0;
        }

        if (_write(fd_, &dh, sizeof(db_header_t)) < 0) {
          return false;
        }

      } else {
        if (_read(fd_, &dh, sizeof(db_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }
      }

      map_ = (char *) _mmap(fd_, DEFAULT_PAGESIZE);

      oflags_ = oflags;
      dh_ = (db_header_t *) map_;
    }

    void *_mmap(int filedes, size_t size, int prot = PROT_READ | PROT_WRITE)
    {
      void *p = mmap(0, size, prot, MAP_SHARED, filedes, 0);  
      if (p == MAP_FAILED) {
        std::cerr << "map failed" << std::endl;
        return NULL;
      }
      return p;
    }

    bool close()
    {
      msync(map_, DEFAULT_PAGESIZE, MS_SYNC);
      munmap(map_, DEFAULT_PAGESIZE);
      ::close(fd_);
    }

    // put new data
    data_ptr_t *put(data_t *data)
    {
      record_t *record = init_record(data);

      // search free area by data size


      // if no free area found
      
      _put(record);
      
      // update data_ptr and statistics
    }

    record_t *init_record(data_t *data)
    {
      uint32_t record_size = sizeof(record_header_t) + data->size;
      uint32_t record_size_ceiled = ceil_size(record_size, 2, 5); // 32bit minimun

      record_t *r = new record_t;
      r->h = new record_header_t;
      r->h->size = record_size;
      r->h->next_block_id = 0; // no succeeding block
      r->d = data;
      r->size_ceiled = record_size_ceiled;
      return r;
    }

    data_ptr_t *_put(record_t *r)
    {
      // allocate blocks more than record size
      uint32_t num_blocks = ceil_size(r->h->size, 2, 12) / dh_->block_size;
      append_blocks(num_blocks);

      // if data size(with record header) is less than page size
      // allocate the size and others are divied into 2^n (n=11,10...)
      // and append them to the free_ptr
      
      // write header
      lseek(fd_, dh_->data_off, SEEK_SET);
      _write(fd_, r->h, sizeof(record_header_t));
      // write data
      lseek(fd_, sizeof(record_header_t), SEEK_CUR);
      _write(fd_, r->d->data, r->d->size);

      if (r->size_ceiled < dh_->block_size) {
        // split the rest and link them to the tails of free ptr
        // chunks = split_block;
        // link_to_freelist(chunks);
      }
    }

    uint32_t ceil_size(uint32_t size, int base, int start_pow)
    {
      for (int i = start_pow; i <= 32; ++i) {
        if (size < pow(base, i)) {
          return pow(base, i);
        }
      }
      return 0;
    }


  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *dh_;

    bool append_blocks(uint32_t append_num_blocks)
    {
      dh_->data_off = DEFAULT_PAGESIZE + dh_->block_size * dh_->num_blocks;
      dh_->num_blocks += append_num_blocks;
      if (ftruncate(fd_, DEFAULT_PAGESIZE + dh_->block_size * dh_->num_blocks) < 0) {
        std::cout << "ftruncate failed" << std::endl;
        return false;
      }
      std::cout << "DEFAULT_PAGESIZE: " << DEFAULT_PAGESIZE << std::endl;
      std::cout << "block_size: " << dh_->block_size << std::endl;
      std::cout << "num_blocks: " << dh_->num_blocks << std::endl;
      return true;
    }

    int _open(const char *pathname, int flags, mode_t mode)
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
    
    ssize_t _read(int fd, void *buf, size_t count)
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

    ssize_t _write(int fd, const void *buf, size_t count)
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

    void _mkdir(const char *str)
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

  };

}

#endif
