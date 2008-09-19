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
  const uint8_t AREA_FREE = 0;
  const uint8_t AREA_ALLOCATED = 1;

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

#pragma pack(2)
  typedef struct {
    block_id_t id;
    uint16_t off;
    uint32_t size;
  } free_pool_ptr_t;
#pragma pack()

#pragma pack(1)
  typedef struct {
    uint8_t type; // allocated or free
    uint32_t size;
    block_id_t next_block_id;
    uint16_t next_off;
    uint32_t next_size;
    bool is_last;
  } free_pool_header_t;

  typedef struct {
    uint8_t type; // allocated or free
    uint32_t size;
    block_id_t next_block_id;
    // key ?
  } record_header_t;
#pragma pack()

  typedef record_header_t common_header_t;

  typedef struct {
    uint32_t num_blocks;
    uint32_t block_size;
    uint32_t bytes_used;
    block_id_t cur_block_id;
    free_pool_ptr_t first_pool_ptr[32]; // by size
    free_pool_ptr_t last_pool_ptr[32];
    bool is_pool_empty[32];
  } db_header_t;

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
          dh.first_pool_ptr[i].id = 0;
          dh.first_pool_ptr[i].off = 0;
          dh.last_pool_ptr[i].id = 0;
          dh.last_pool_ptr[i].off = 0;
          dh.is_pool_empty[i] = true;
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

    void show_free_pools(void)
    {
      for (int i = 0; i < 32; ++i) {
        if (!dh_->is_pool_empty[i]) {
          free_pool_ptr_t pool_ptr = dh_->first_pool_ptr[i];
          std::cout << "free pool size: " << pow(2, i+1) << std::endl;
          while (1) {
            off_t off = calc_off(pool_ptr.id, pool_ptr.off);
            std::cout << "block id: " << pool_ptr.id 
                      << ", off: " << pool_ptr.off
                      << ", size: " << pool_ptr.size << std::endl;
              
            free_pool_header_t pool_header;
            _pread(fd_, &pool_header, sizeof(free_pool_header_t), off);
            std::cout << "next id: " << pool_header.next_block_id 
                      << ", next off: " << pool_header.next_off 
                      << ", next size: " << pool_header.next_size 
                      << ", is_last: " << pool_header.is_last << std::endl;
            if (pool_header.is_last) { break; }
            // block id and offset for next free pool
            pool_ptr.id = pool_header.next_block_id;
            pool_ptr.off = pool_header.next_off;
          }
        }
      }
    }

    void show_db_header(void)
    {
      std::cout << "=== DATABASE HEADER ===" << std::endl;
      std::cout << "num_blocks: " << dh_->num_blocks << std::endl;
      std::cout << "current block id: " << dh_->cur_block_id << std::endl;
    }

    // put new data
    data_ptr_t *put(data_t *data)
    {
      data_ptr_t *data_ptr;
      record_t *r = init_record(data);
/*
      std::cout << "data size: " << r->d->size << std::endl;  
      std::cout << "size (header+data): " << r->h->size << std::endl;  
      std::cout << "size (ceiled): " << r->size_ceiled << std::endl;  
*/

      // search free area by data size
      free_pool_ptr_t *pool = get_free_pool(r);
      if (pool != NULL) {
        //std::cout << "####### pool found !!! #######" << std::endl;
        
        data_ptr = write_record(r, pool->id, pool->off);
        append_free_pool(pool->id, pool->off + r->size_ceiled,
                         pool->size - r->size_ceiled);

      } else {
        // no free pool found
        data_ptr = _put(r);
      }
      
      // update data_ptr and statistics
      //

      return data_ptr;
    }

    data_t *get(data_ptr_t *data_ptr)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      data_t *data = new data_t;
      data->data = new char[h.size - sizeof(record_header_t)];
      data->size = h.size - sizeof(record_header_t);
      int nbytes = _pread(fd_, (char *) data->data, data->size, 
                          off + sizeof(record_header_t));

      if (nbytes != data->size) {
        return NULL;
      }
      /*
      std::cout << "data size: [" << nbytes << "]" << std::endl;
      std::cout << "record size: " << h.size << std::endl;
      std::cout << "succeeding block id: " << h.next_block_id << std::endl;
      */
      return data;
    }

    free_pool_ptr_t *get_free_pool(record_t *r)
    {
      uint32_t pow = get_pow_ceiled(r->size_ceiled, 2, 5);

      // search a pool
      bool pool_found = false;
      while (pow <= 32) {
        if (!dh_->is_pool_empty[pow-1]) {
          pool_found = true;
          break;
        }
        ++pow;
      }
      if (!pool_found) {
        return NULL;
      }

      free_pool_ptr_t *pool = new free_pool_ptr_t;
      free_pool_ptr_t *first_pool = &(dh_->first_pool_ptr[pow-1]);
      memcpy(pool, first_pool, sizeof(free_pool_ptr_t));

      //std::cout << "free pool found. size: " << first_pool->size << std::endl;
  
      // organize free pool pointers
      free_pool_header_t pool_header;
      off_t off = calc_off(first_pool->id, first_pool->off);
      _pread(fd_, &pool_header, sizeof(free_pool_header_t), off);
      if (pool_header.is_last) {
        dh_->is_pool_empty[pow-1] = true;  
      } else {
        first_pool->id = pool_header.next_block_id;
        first_pool->off = pool_header.next_off;
        first_pool->size = pool_header.next_size;
      }

      return pool;
    }

    record_t *init_record(data_t *data)
    {
      uint32_t record_size = sizeof(record_header_t) + data->size;
      uint32_t pow_ceiled =  get_pow_ceiled(record_size, 2, 5);

      record_t *r = new record_t;
      r->h = new record_header_t;
      r->h->type = AREA_ALLOCATED;
      r->h->size = record_size;
      r->h->next_block_id = 0; // no succeeding block
      r->d = data;
      // [TODO] ceiling is done in two ways (2^n or with padding)
      // [TODO] if the size is more than block size, size_ceiled is selected in two ways.
      // (block*n or the rest is going to pool)
      //r->size_ceiled = pow(2, pow_ceiled);
      r->size_ceiled = record_size;
      return r;
    }

    data_ptr_t *_put(record_t *r)
    {
      // allocate blocks more than record size
      div_t d = div(r->size_ceiled, dh_->block_size);
      uint32_t num_blocks = d.rem > 0 ? d.quot + 1 : d.quot;
      append_blocks(num_blocks);

      //write a record into the head of the block
      data_ptr_t *data_ptr = write_record(r, dh_->cur_block_id, 0);
      if (data_ptr == NULL) {
        return NULL;
      }

      if (r->size_ceiled < dh_->block_size) {
        append_free_pool(dh_->cur_block_id, r->size_ceiled,
                         dh_->block_size - r->size_ceiled);
      } else {
        if (d.rem > 0) {
          append_free_pool(dh_->cur_block_id + d.quot,
                           d.rem, dh_->block_size - d.rem);
        }
      }

      return data_ptr;
    }

    void append_free_pool(block_id_t block_id, uint16_t off_in_block, uint16_t size)
    {
      for (int i = 11; i >= 5; --i) { 
        uint16_t chunk_size = pow(2, i);
        if (size >= chunk_size) {
          /*
          std::cout << "_append_free_pool: "
                    << "block_id: " << block_id 
                    << ", off: " << off_in_block 
                    << ", size: " << size
                    << ", pow: " << i << std::endl; 
                    */
          _append_free_pool(block_id, off_in_block, size, i);
          break;
        }
      }
    }
  
    void _append_free_pool(block_id_t id, uint16_t off_in_block, uint32_t size, int pow)
    {
      // added size to free_pool_header_t
      free_pool_ptr_t ptr = {id, off_in_block, size};
      if (dh_->is_pool_empty[pow-1]) {
        memcpy(&(dh_->first_pool_ptr[pow-1]), &ptr, sizeof(free_pool_ptr_t));
        dh_->is_pool_empty[pow-1] = false;
      } else {
        free_pool_ptr_t last_ptr;
        memcpy(&last_ptr, &(dh_->last_pool_ptr[pow-1]), sizeof(free_pool_ptr_t));
        // put ptr to the next(last) pool
        off_t off = calc_off(last_ptr.id, last_ptr.off);
        free_pool_header_t header = {AREA_FREE, last_ptr.size, id, off_in_block, size, false};
        _pwrite(fd_, &header, sizeof(free_pool_header_t), off);
      }
      // last pool
      free_pool_header_t header = {AREA_FREE, size, 0, 0, 0, true};
      off_t off = calc_off(id, off_in_block);
      _pwrite(fd_, &header, sizeof(free_pool_header_t), off);
      memcpy(&(dh_->last_pool_ptr[pow-1]), &ptr, sizeof(free_pool_ptr_t));
    }

    data_ptr_t *write_record(record_t *r, block_id_t block_id, uint16_t off)
    {
      off_t g_off = calc_off(block_id, off);
      int nbytes = _pwrite(fd_, r->h, sizeof(record_header_t), g_off);
      nbytes += _pwrite(fd_, r->d->data, r->d->size, g_off + sizeof(record_header_t));

      if (nbytes != sizeof(record_header_t) + r->d->size) {
        return NULL;
      }
      data_ptr_t *data_ptr = new data_ptr_t;  
      data_ptr->id = block_id;
      data_ptr->off = off;
      return data_ptr;
    }

    off_t calc_off(block_id_t id, uint16_t off)
    {
      return (id-1) * dh_->block_size + DEFAULT_PAGESIZE + off;
    }

    uint32_t get_pow_ceiled(uint32_t size, int base, int start_pow)
    {
      for (int i = start_pow; i <= 32; ++i) {
        if (size <= pow(base, i)) {
          return i;
        }
      }
      return 0;
    }

    uint32_t get_pow_floored(uint32_t size, int base, int start_pow)
    {
      for (int i = start_pow; i >= 1; --i) {
        if (size >= pow(base, i)) {
          return i;
        }
      }
      return 0;
    }

    uint32_t ceil_size(uint32_t size, int base, int start_pow)
    {
      for (int i = start_pow; i <= 32; ++i) {
        if (size <= pow(base, i)) {
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
      dh_->cur_block_id = dh_->num_blocks + 1;
      dh_->num_blocks += append_num_blocks;
      if (ftruncate(fd_, DEFAULT_PAGESIZE + dh_->block_size * dh_->num_blocks) < 0) {
        std::cout << "ftruncate failed" << std::endl;
        return false;
      }
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

    ssize_t _pwrite(int fd, const void *buf, size_t nbyte, off_t offset)
    {
      lseek(fd, offset, SEEK_SET);
      return _write(fd, buf, nbyte);
      // [TODO] pwrite 
    }

    ssize_t _pread(int fd, void *buf, size_t nbyte, off_t offset)
    {
      lseek(fd, offset, SEEK_SET);
      return _read(fd, buf, nbyte);
      // [TODO] pread
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
