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
  const uint32_t DEFAULT_PADDING = 20; // depends on the context

  typedef enum {
    NOPADDING,
    FIXEDLEN,
    RATIO,
    PO2 // power of 2
  } padding_mode_t;

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

#pragma pack(1)
    typedef struct {
      uint8_t type; // allocated or free
      uint32_t size;
      block_id_t next_block_id;
      uint16_t next_off;
      uint32_t next_size;
      bool is_last;
    } free_pool_header_t;
#pragma pack()

#pragma pack(2)
    typedef struct {
      block_id_t id;
      uint16_t off;
      uint32_t size;
    } free_pool_ptr_t;
#pragma pack()

    typedef struct {
      uint32_t num_blocks;
      uint32_t block_size;
      uint32_t bytes_used;
      block_id_t cur_block_id;
      padding_mode_t pmode;
      uint32_t padding;
      free_pool_ptr_t first_pool_ptr[32]; // by size
      free_pool_ptr_t last_pool_ptr[32];
      bool is_pool_empty[32];
    } db_header_t;
    
  /*
   * Class Data
   */
  class Data {

    struct record_t;
    struct record_header_t;

  public:
    Data(padding_mode_t pmode = PO2, uint32_t padding = DEFAULT_PADDING)
    : pmode_(pmode), padding_(padding)
    {}

    virtual ~Data()
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
        dh.cur_block_id = 0;
        dh.pmode = pmode_;
        dh.padding = padding_;
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

    bool close()
    {
      msync(map_, DEFAULT_PAGESIZE, MS_SYNC);
      munmap(map_, DEFAULT_PAGESIZE);
      ::close(fd_);
    }

    void set_padding(padding_mode_t pmode, uint32_t padding = DEFAULT_PADDING)
    {
      pmode_ = pmode;
      padding_ = padding;
    }

    void show_free_pools(void)
    {
      for (int i = 0; i < 32; ++i) {
        if (!dh_->is_pool_empty[i]) {
          free_pool_ptr_t pool_ptr = dh_->first_pool_ptr[i];
          std::cout << "free pool size: " << pow(2, i+1) << std::endl;
          while (1) {
            off_t off = calc_off(pool_ptr.id, pool_ptr.off);
              
            free_pool_header_t pool_header;
            _pread(fd_, &pool_header, sizeof(free_pool_header_t), off);
            std::cout << "size: " << pool_header.size
                      << ", next id: " << pool_header.next_block_id 
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

    void clean_data_ptr(data_ptr_t *data_ptr)
    {
      delete data_ptr;
    }

    void clean_data(data_t *data)
    {
      delete [] (char *) data->data;
      delete data;
    }

    virtual data_ptr_t *put(data_t *data) = 0;
    virtual data_ptr_t *append(data_ptr_t *data_ptr, data_t *data) = 0;
    virtual data_ptr_t *update(data_ptr_t *data_ptr, data_t *data) = 0;
    virtual void del(data_ptr_t *data_ptr) = 0;
    virtual data_t *get(data_ptr_t *data_ptr) = 0;

  protected:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *dh_;
    padding_mode_t pmode_;
    uint32_t padding_;

    bool search_free_pool(uint32_t size, free_pool_ptr_t *pool)
    {
      uint32_t pow = get_pow_ceiled(size, 2, 5);

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
        return false;
      }

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

      return true;
    }

    uint32_t get_padding(uint32_t size)
    {
      switch (dh_->pmode) {
        case NOPADDING:
          return size;
        case FIXEDLEN:
          return size + dh_->padding;
        case RATIO:
          return size + size * dh_->padding / 100;
        default: // PO2
          return pow(2, get_pow_ceiled(size, 2, 5));
      }
    }

    void append_free_pool(block_id_t block_id, uint16_t off_in_block, uint16_t size)
    {
      bool is_appended = false;
      for (int i = 11; i >= 5; --i) { 
        if (size >= pow(2, i)) {
          /*
          std::cout << "_append_free_pool: "
                    << "block_id: " << block_id 
                    << ", off: " << off_in_block 
                    << ", size: " << size
                    << ", pow: " << i << std::endl; 
                    */
          _append_free_pool(block_id, off_in_block, size, i);
          is_appended = true;
          break;
        }
      }

      // too small chunk remains unused
      if (!is_appended) {
        // [TODO]
        //std::cout << size << " bytes in block id: " << block_id << " is unused." << std::endl;
      }
    }

    void prepend_free_pool(block_id_t id, uint16_t off_in_block, uint32_t size, int pow)
    {
      // [TODO]
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

    // system call wrappers
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

    void *_mmap(int filedes, size_t size, int prot = PROT_READ | PROT_WRITE)
    {
      void *p = mmap(0, size, prot, MAP_SHARED, filedes, 0);  
      if (p == MAP_FAILED) {
        std::cerr << "map failed" << std::endl;
          return NULL;
      }
      return p;
    }
  };

  /*
   * Class PaddedData
   */
  class PaddedData : public Data {

#pragma pack(1)
    typedef struct {
      uint8_t type; // allocated or free
      uint32_t size;
      uint32_t size_padded; // size in a block
    } record_header_t;
#pragma pack()

    typedef struct {
      record_header_t *h;
      data_t *d; 
    } record_t;

  public:
    PaddedData(padding_mode_t pmode = PO2, uint32_t padding = DEFAULT_PADDING)
    : Data(pmode, padding)
    {}
    ~PaddedData() {}

    // put new data
    data_ptr_t *put(data_t *data)
    {
      data_ptr_t *data_ptr;
      record_t *r = init_record(data);
      r->h->size_padded = get_padding(r->h->size);
/*
      std::cout << "data size: " << r->d->size << std::endl;  
      std::cout << "size (header+data): " << r->h->size << std::endl;  
      std::cout << "size (ceiled): " << r->h->size_padded << std::endl;  
*/

      // search free area by data size
      free_pool_ptr_t pool;
      if (search_free_pool(r->h->size_padded, &pool)) {
        //std::cout << "####### pool found !!! #######" << std::endl;
       
        // [TODO] if the (pool.size - r->h->size_padded) < 32) => r->h->size_padded = pool.size
        data_ptr = write_record(r, pool.id, pool.off);
        append_free_pool(pool.id, pool.off + r->h->size_padded,
                         pool.size - r->h->size_padded);

      } else {
        // no free pool found
        data_ptr = _put(r);
      }

      delete r->h;
      delete r;

      return data_ptr;
    }

    data_ptr_t *append(data_ptr_t *data_ptr, data_t *data)
    {
      data_ptr_t *ptr;
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      if (h.size_padded - h.size >= data->size) {
        // if the padding is big enough
        int bytes_write = _pwrite(fd_, data->data, data->size, off + h.size);
        if (bytes_write != data->size) { return NULL; }

        // update the header
        h.size += bytes_write;
        _pwrite(fd_, &h, sizeof(record_header_t), off);

        // pointer unchanged
        ptr = data_ptr;

      } else {
        data_t d;
        // [TODO] size should specify the size of data (not including header)
        uint32_t prev_size = h.size - sizeof(record_header_t);
        d.size = prev_size + data->size;
        d.data = new char[d.size];
        _pread(fd_, (char *) d.data, prev_size, off + sizeof(record_header_t));
        memcpy((char *) d.data + prev_size, data->data, data->size);

        // record moved, so pointing another place
        ptr = put(&d);

        // previously used area is put into free pools
        append_free_pool(data_ptr->id, data_ptr->off, h.size_padded);

        delete [] (char *) d.data;
      }
      return ptr;
    }

    data_ptr_t *update(data_ptr_t *data_ptr, data_t *data)
    {
      data_ptr_t *ptr;
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      if (h.size_padded - sizeof(record_header_t) >= data->size) {
        // if the padding is big enough
        int bytes_write = _pwrite(fd_, data->data, data->size,
                                  off + sizeof(record_header_t)); 
        if (bytes_write != data->size) { return NULL; }

        // update the header
        h.size = data->size + sizeof(record_header_t);
        _pwrite(fd_, &h, sizeof(record_header_t), off);

        // pointer unchanged
        ptr = data_ptr;

      } else {
        // record moved, so pointing another place
        ptr = put(data);

        // previously used area is put into free pools
        append_free_pool(data_ptr->id, data_ptr->off, h.size_padded);
      }
      return ptr;
    }

    void del(data_ptr_t *data_ptr)
    {
      // deleted chunk is put into free pools
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      append_free_pool(data_ptr->id, data_ptr->off, h.size_padded);
    }

    data_t *get(data_ptr_t *data_ptr)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      data_t *data = new data_t;
      data->data = new char[h.size - sizeof(record_header_t)];
      data->size = h.size - sizeof(record_header_t);
      int bytes_read = _pread(fd_, (char *) data->data, data->size, 
                              off + sizeof(record_header_t));

      if (bytes_read != data->size) {
        return NULL;
      }
      return data;
    }

  private:
    data_ptr_t *_put(record_t *r)
    {
      // allocate blocks more than record size
      div_t d = div(r->h->size_padded, dh_->block_size);
      uint32_t num_blocks = d.rem > 0 ? d.quot + 1 : d.quot;
      append_blocks(num_blocks);

      //write a record into the head of the block
      data_ptr_t *data_ptr = write_record(r, dh_->cur_block_id, 0);
      if (data_ptr == NULL) {
        return NULL;
      }

      if (r->h->size_padded < dh_->block_size) {
        append_free_pool(dh_->cur_block_id, r->h->size_padded,
                         dh_->block_size - r->h->size_padded);
      } else {
        if (d.rem > 0) {
          append_free_pool(dh_->cur_block_id + d.quot,
                           d.rem, dh_->block_size - d.rem);
        }
      }

      return data_ptr;
    }

    data_ptr_t *write_record(record_t *r, block_id_t block_id, uint16_t off)
    {
      off_t g_off = calc_off(block_id, off);
      int bytes_write = _pwrite(fd_, r->h, sizeof(record_header_t), g_off);
      bytes_write += _pwrite(fd_, r->d->data, r->d->size, g_off + sizeof(record_header_t));

      if (bytes_write != sizeof(record_header_t) + r->d->size) {
        return NULL;
      }
      data_ptr_t *data_ptr = new data_ptr_t;  
      data_ptr->id = block_id;
      data_ptr->off = off;
      return data_ptr;
    }

    record_t *init_record(data_t *data)
    {
      record_t *r = new record_t;
      record_header_t *h = new record_header_t;

      h->type = AREA_ALLOCATED;
      h->size = sizeof(record_header_t) + data->size;
      h->size_padded = h->size;
      r->h = h;
      r->d = data;

      return r;
    }
  };

  /*
   * Class LinkedData
   */
  class LinkedData : public Data {

#pragma pack(1)
    typedef struct {
      uint8_t type; // allocated or free
      uint32_t size;
      uint32_t size_padded; // size in a block
      /*
      block_id_t next_block_id;
      uint16_t next_off;
      uint32_t size_data_total;
      block_id_t last_block_id;
      uint8_t num_succeedings;
      */
    } record_header_t;
#pragma pack()

    typedef struct {
      record_header_t *h;
      data_t *d; 
    } record_t;

  public:
    // put new data
    data_ptr_t *put(data_t *data)
    {
      data_ptr_t *data_ptr;
      record_t *r = init_record(data);
      //r->size_padded = put_padding(r->size);
/*
      std::cout << "data size: " << r->d->size << std::endl;  
      std::cout << "size (header+data): " << r->h->size << std::endl;  
      std::cout << "size (ceiled): " << r->h->size_padded << std::endl;  
*/
      // search free area by data size
      free_pool_ptr_t pool;
      if (search_free_pool(r->h->size_padded, &pool)) {
        //std::cout << "####### pool found !!! #######" << std::endl;
       
        // [TODO] if the (pool.size - r->h->size_padded) < 32) => r->h->size_padded = pool.size
        data_ptr = write_record(r, pool.id, pool.off);
        append_free_pool(pool.id, pool.off + r->h->size_padded,
                         pool.size - r->h->size_padded);

      } else {
        // no free pool found
        data_ptr = _put(r);
      }

      delete r->h;
      delete r;

      return data_ptr;
    }

    data_ptr_t *append(data_ptr_t *data_ptr, data_t *data)
    {
      /*
      // first, try to put the date into the padding
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      _pread(fd_, &h, sizeof(record_header_t), off);

      // if the padding is big enough, then no big deal. just put the data into it.
      if (h.is_linkable) {
        extended_record_header_t eh;
        _pread(fd_, &eh, sizeof(extended_record_header_t), off + sizeof(record_header_t));
        record_header_t lh;
        off_t last_header_off = calc_off(eh.last_block_id, 0);
        _pread(fd_, &lh, sizeof(record_header_t), last_header_off);

        uint32_t rest_size = lh.size_padded - lh.size;
        if (rest_size >= data->size) {
          // just append
          // [TODO] adding append_record method ?
          _pwrite(fd_, data->data, data->size, last_header_off + lh.size);
          lh.size += data->size;
        } else {
          _pwrite(fd_, data->data, rest_size, last_header_off + lh.size);
          data_t new_data = { (char *) data + rest_size, data->size - rest_size };
          record_t *r = init_record(new_data); 
          r->h->size_padded = lh.size_padded; // the same size
          // [TODO] block aligned ?
          data_ptr_t *data_ptr = put(r);
          lh.size = lh.size_padded; // full
          lh.next_block_id = data_ptr->id;

          delete r->h;
          delete r;
        }
        // sync lh
        // sync h
      } else {
        if (h.size_padded - h.size >= data->size) {
          // just append
          _pwrite(fd_, data->data, data->size, off + h.size);
          h.size += data->size;
          _pwrite(fd_, &h, sizeof(record_header_t), off);
        } else {
          // del
          // put
        }
      }
      */
    }

    data_ptr_t *update(data_ptr_t *data_ptr, data_t *data)
    {
      // update is very simple

      // first, try to put the date into the padding

      // if the padding is big enough, update the data

      // else, create a new record and put the data into it.
      // current record area is appended to free pools.
    }

    void del(data_ptr_t *data_ptr)
    {
      // keep deleting the record and keep appending it to free pools
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

  private:

    data_ptr_t *_put(record_t *r)
    {
      // allocate blocks more than record size
      div_t d = div(r->h->size_padded, dh_->block_size);
      uint32_t num_blocks = d.rem > 0 ? d.quot + 1 : d.quot;
      append_blocks(num_blocks);

      //write a record into the head of the block
      data_ptr_t *data_ptr = write_record(r, dh_->cur_block_id, 0);
      if (data_ptr == NULL) {
        return NULL;
      }

      if (r->h->size_padded < dh_->block_size) {
        append_free_pool(dh_->cur_block_id, r->h->size_padded,
                         dh_->block_size - r->h->size_padded);
      } else {
        if (d.rem > 0) {
          append_free_pool(dh_->cur_block_id + d.quot,
                           d.rem, dh_->block_size - d.rem);
        }
      }

      return data_ptr;
    }

    data_ptr_t *write_record(record_t *r, block_id_t block_id, uint16_t off)
    {
      off_t g_off = calc_off(block_id, off);
      int bytes_write = _pwrite(fd_, r->h, sizeof(record_header_t), g_off);
      bytes_write += _pwrite(fd_, r->d->data, r->d->size, g_off + sizeof(record_header_t));

      if (bytes_write != sizeof(record_header_t) + r->d->size) {
        return NULL;
      }
      data_ptr_t *data_ptr = new data_ptr_t;  
      data_ptr->id = block_id;
      data_ptr->off = off;
      return data_ptr;
    }

    record_t *init_record(data_t *data)
    {
      record_t *r = new record_t;
      record_header_t *h = new record_header_t;

      h->type = AREA_ALLOCATED;
      h->size = sizeof(record_header_t) + data->size;
      h->size_padded = h->size;
      /*
      h->next_block_id = 0;
      h->size_data_total = data->size;
      h->last_block_id = 0;
      h->num_succeedings = 0;
      */
      r->h = h;
      r->d = data;

      return r;
    }
  };

}

#endif