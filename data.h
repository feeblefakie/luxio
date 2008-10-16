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

#ifndef LUX_DBM_DATA_H
#define LUX_DBM_DATA_H

#include "dbm.h"

namespace Lux {
namespace DBM {

  const static uint8_t AREA_FREE = 0;
  const static uint8_t AREA_ALLOCATED = 1;
  const static uint32_t DEFAULT_PADDING = 20; // depends on the context
  const static uint32_t MIN_RECORD_SIZE = 32;

  typedef enum {
    Padded,
    Linked
  } store_mode_t;

  typedef enum {
    NOPADDING,
    FIXEDLEN,
    RATIO,
    PO2 // power of 2
  } padding_mode_t;

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
      store_mode_t smode;
      free_pool_ptr_t first_pool_ptr[32]; // by size
      free_pool_ptr_t last_pool_ptr[32];
      bool is_pool_empty[32];
    } db_header_t;
    
  /*
   * Class Data
   */
  class Data {

    struct record_header_t;
    struct record_t;

  public:
    Data(store_mode_t smode,
         padding_mode_t pmode = PO2, uint32_t padding = DEFAULT_PADDING)
    : smode_(smode),
      pmode_(pmode),
      padding_(padding),
      page_size_(getpagesize()),
      header_size_(getpagesize()),
      map_(NULL)
    {
      // calculate each 2^i (1<=i<=32) in advance
      for (int i = 0; i < 32; ++i) {
        pows_[i] = pow(2, i+1);
      }
    }

    virtual ~Data()
    {
      if (map_ != NULL) {
        close();
      }
    }

    bool open(std::string db_name, db_flags_t oflags)
    {
      fd_ = _open(db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        ERROR_LOG("open failed.");
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd_, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        ERROR_LOG("fstat failed.");
        return false;
      }

      db_header_t dh;
      memset(&dh, 0, sizeof(db_header_t));
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        dh.num_blocks = 0;
        dh.block_size = page_size_;
        dh.bytes_used = 0;
        dh.cur_block_id = 0;
        dh.pmode = pmode_;
        dh.padding = padding_;
        dh.smode = smode_;
        for (int i = 0; i < 32; ++i) {
          dh.first_pool_ptr[i].id = 0;
          dh.first_pool_ptr[i].off = 0;
          dh.last_pool_ptr[i].id = 0;
          dh.last_pool_ptr[i].off = 0;
          dh.is_pool_empty[i] = true;
        }

        if (_write(fd_, &dh, sizeof(db_header_t)) < 0) {
          ERROR_LOG("write failed.");
          return false;
        }

      } else {
        if (_read(fd_, &dh, sizeof(db_header_t)) < 0) {
          ERROR_LOG("read failed");
          return false;
        }

        if (dh.smode != smode_) {
          ERROR_LOG("opening wrong database type");
          return false;
        }
      }

      map_ = (char *) _mmap(fd_, header_size_, oflags);

      oflags_ = oflags;
      dh_ = (db_header_t *) map_;
      return true;
    }

    bool close()
    {
      if (map_ != NULL) {
        if (msync(map_, header_size_, MS_SYNC) < 0) {
          ERROR_LOG("msync failed.");
          return false;
        }
        if (munmap(map_, header_size_) < 0) {
          ERROR_LOG("munmap failed.");
          return false;
        }
      }
      if (::close(fd_) < 0) {
        ERROR_LOG("close failed.");
        return false;
      }
      return true;
    }

    void set_padding(padding_mode_t pmode, uint32_t padding = DEFAULT_PADDING)
    {
      pmode_ = pmode;
      padding_ = padding;
    }

    void set_page_size(uint32_t page_size)
    {
      if (page_size > MAX_PAGESIZE || 
          page_size < MIN_PAGESIZE) {
        return;
      }
      page_size_ = page_size;
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
      std::cout << "----- DATABASE HEADER -----" << std::endl
                << "num_blocks: " << dh_->num_blocks << std::endl
                << "current block id: " << dh_->cur_block_id << std::endl;
    }

    void clean_data_ptr(data_ptr_t *data_ptr)
    {
      delete data_ptr;
    }

    void clean_data(data_t *data)
    {
      if (data != NULL) {
        delete [] (char *) data->data;
        delete data;
        data = NULL;
      }
    }

    virtual data_ptr_t *put(data_t *data) = 0;
    virtual data_ptr_t *append(data_ptr_t *data_ptr, data_t *data) = 0;
    virtual data_ptr_t *update(data_ptr_t *data_ptr, data_t *data) = 0;
    virtual bool del(data_ptr_t *data_ptr) = 0;
    virtual data_t *get(data_ptr_t *data_ptr) = 0;
    // using user memory
    virtual bool get(data_ptr_t *data_ptr, data_t *data, uint32_t *size) = 0;

  protected:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *dh_;
    padding_mode_t pmode_;
    uint32_t padding_;
    store_mode_t smode_;
    uint32_t page_size_;
    uint32_t header_size_;
    double pows_[32];

    bool search_free_pool(uint32_t size, free_pool_ptr_t *pool)
    {
      uint32_t pow = get_pow_of_2_ceiled(size, 5);

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

      // organize free pool pointers
      free_pool_header_t pool_header;
      off_t off = calc_off(first_pool->id, first_pool->off);
      if (!_pread(fd_, &pool_header, sizeof(free_pool_header_t), off)) {
        return false;
      }
      if (pool_header.is_last) {
        dh_->is_pool_empty[pow-1] = true;  
      } else {
        first_pool->id = pool_header.next_block_id;
        first_pool->off = pool_header.next_off;
        first_pool->size = pool_header.next_size;
      }

      return true;
    }

    uint32_t get_padded_size(uint32_t size)
    {
      uint32_t padded_size;
      switch (dh_->pmode) {
        case NOPADDING:
          padded_size = size;
          break;
        case FIXEDLEN:
          padded_size = size + dh_->padding;
          break;
        case RATIO:
          padded_size = size + size * dh_->padding / 100;
          break;
        default: // PO2
          padded_size = (uint32_t) pows_[get_pow_of_2_ceiled(size, 5)-1];
      }
      if (padded_size < MIN_RECORD_SIZE) {
        padded_size = MIN_RECORD_SIZE;
      }
      return padded_size;
    }

    bool add_free_pool(block_id_t block_id, uint16_t off_in_block, uint32_t size)
    {
      bool is_appended = false;
      for (int i = 32; i >= 5; --i) { 
        if (size >= pows_[i-1]) {
          if (!_prepend_free_pool(block_id, off_in_block, size, i)) {
            return false;
          }
          is_appended = true;
          break;
        }
      }

      // too small chunk remains unused
      if (!is_appended) {
        // [TODO]
        //std::cout << size << " bytes in block id: " << block_id << " is unused." << std::endl;
      }

      return true;
    }

    bool _prepend_free_pool(block_id_t id, uint16_t off_in_block, uint32_t size, int pow)
    {
      free_pool_ptr_t ptr = {id, off_in_block, size};
      off_t off = calc_off(id, off_in_block);
      if (dh_->is_pool_empty[pow-1]) {
        // first pool is also pointed by last ptr
        memcpy(&(dh_->last_pool_ptr[pow-1]), &ptr, sizeof(free_pool_ptr_t));
        dh_->is_pool_empty[pow-1] = false;

        // write the header of the pool
        free_pool_header_t header = {AREA_FREE, size, 0, 0, 0, true};
        if (!_pwrite(fd_, &header, sizeof(free_pool_header_t), off)) {
          return false;
        }
      } else {
        free_pool_ptr_t first_ptr;
        memcpy(&first_ptr, &(dh_->first_pool_ptr[pow-1]), sizeof(free_pool_ptr_t));

        // write the header of the pool
        free_pool_header_t header = {AREA_FREE, size, first_ptr.id,
                                     first_ptr.off, first_ptr.size, false};
        if (!_pwrite(fd_, &header, sizeof(free_pool_header_t), off)) {
          return false;
        }
      }
      memcpy(&(dh_->first_pool_ptr[pow-1]), &ptr, sizeof(free_pool_ptr_t));

      return true;
    }
  
    bool _append_free_pool(block_id_t id, uint16_t off_in_block, uint32_t size, int pow)
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
        if (!_pwrite(fd_, &header, sizeof(free_pool_header_t), off)) {
          return false;
        }
      }
      // last pool
      free_pool_header_t header = {AREA_FREE, size, 0, 0, 0, true};
      off_t off = calc_off(id, off_in_block);
      if (!_pwrite(fd_, &header, sizeof(free_pool_header_t), off)) {
        return false;
      }
      memcpy(&(dh_->last_pool_ptr[pow-1]), &ptr, sizeof(free_pool_ptr_t));

      return true;
    }

    off_t calc_off(block_id_t id, uint16_t off)
    {
      return (id-1) * dh_->block_size + header_size_ + off;
    }

    uint32_t get_pow_of_2_ceiled(uint32_t size, int start)
    {
      for (int i = start; i <= 32; ++i) {
        if (size <= pows_[i-1]) {
          return i;
        }
      }
      return 0;
    }

    bool extend_blocks(uint32_t append_num_blocks)
    {
      dh_->cur_block_id = dh_->num_blocks + 1;
      dh_->num_blocks += append_num_blocks;
      if (ftruncate(fd_, header_size_ + dh_->block_size * dh_->num_blocks) < 0) {
        ERROR_LOG("ftruncate failed.");
        return false;
      }
      return true;
    }

    template<typename T>
    bool write_header_and_data(T *h, data_t *d, off_t off)
    {
      // write headers and data
      if (!_pwrite(fd_, h, sizeof(T), off)) {
        return false;
      }
      if (!_pwrite(fd_, d->data, d->size, off + sizeof(T))) {
        return false;
      }
      return true;
    }

    data_ptr_t *alloc_space(uint32_t size)
    {
      data_ptr_t *data_ptr = new data_ptr_t;
      free_pool_ptr_t pool;
      if (search_free_pool(size, &pool)) {
        data_ptr->id = pool.id;
        data_ptr->off = pool.off;
        if (!add_free_pool(pool.id, pool.off + size, pool.size - size)) {
          return NULL;
        }
      } else {
        div_t d = div(size, dh_->block_size);
        uint32_t num_blocks = d.rem > 0 ? d.quot + 1 : d.quot;
        if (!extend_blocks(num_blocks)) { return NULL; }

        //write a record into the head of the block
        data_ptr->id = dh_->cur_block_id;
        data_ptr->off = 0;

        if (size < dh_->block_size) {
          if (!add_free_pool(dh_->cur_block_id, size, dh_->block_size - size)) {
            return NULL;
          }
        } else {
          if (d.rem > 0) {
            if (!add_free_pool(dh_->cur_block_id + d.quot,
                               d.rem, dh_->block_size - d.rem)) {
              return NULL;
            }
          }
        }
      }
      return data_ptr;
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
      uint32_t padded_size; // size in a block
    } record_header_t;
#pragma pack()

    typedef struct {
      record_header_t *h;
      data_t *d; 
    } record_t;

  public:
    PaddedData(padding_mode_t pmode = PO2, uint32_t padding = DEFAULT_PADDING)
    : Data(Padded, pmode, padding)
    {}
    virtual ~PaddedData() {}

    virtual data_ptr_t *put(data_t *data)
    {
      record_t *r = init_record(data);

      data_ptr_t *data_ptr = alloc_space(r->h->padded_size);
      if (data_ptr == NULL) {
        ERROR_LOG("alloc_space failed.");
        return NULL;
      }
      //write a record into the head of the block
      if (!write_record(r, data_ptr)) {
        return NULL;
      }

      clear_record(r);
      return data_ptr;
    }

    virtual data_ptr_t *append(data_ptr_t *data_ptr, data_t *data)
    {
      data_ptr_t *ptr;
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return NULL;
      }
      if (h.type == AREA_FREE) {
        ERROR_LOG("data_ptr area is not marked free.");
        return NULL;
      }

      if (h.padded_size - h.size >= data->size) {
        // if the padding is big enough
        if (!_pwrite(fd_, data->data, data->size, off + h.size)) {
          return NULL;
        }
        // update the header
        h.size += data->size;
        if (!_pwrite(fd_, &h, sizeof(record_header_t), off)) {
          return NULL;
        }
        // pointer unchanged
        ptr = new data_ptr_t;
        memcpy(ptr, data_ptr, sizeof(data_ptr_t));

      } else {
        data_t d;
        uint32_t prev_size = h.size - sizeof(record_header_t);
        d.size = prev_size + data->size;
        d.data = new char[d.size];
        if (!_pread(fd_, (char *) d.data, prev_size, off + sizeof(record_header_t))) {
          return NULL;
        }
        memcpy((char *) d.data + prev_size, data->data, data->size);

        // record moved, so pointing another place
        ptr = put(&d);

        // previously used area is put into free pools
        if (!add_free_pool(data_ptr->id, data_ptr->off, h.padded_size)) {
          return NULL;
        }

        delete [] (char *) d.data;
      }
      return ptr;
    }

    virtual data_ptr_t *update(data_ptr_t *data_ptr, data_t *data)
    {
      data_ptr_t *ptr;
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return NULL;
      }
      if (h.type == AREA_FREE) { return NULL; }

      if (h.padded_size - sizeof(record_header_t) >= data->size) {
        // if the padding is big enough
        if (!_pwrite(fd_, data->data, data->size,
                     off + sizeof(record_header_t))) {
          return NULL;
        }
        // update the header
        h.size = data->size + sizeof(record_header_t);
        if (!_pwrite(fd_, &h, sizeof(record_header_t), off)) {
          return NULL;
        }
        // pointer unchanged
        ptr = new data_ptr_t;
        memcpy(ptr, data_ptr, sizeof(data_ptr_t));

      } else {
        // record moved, so pointing another place
        ptr = put(data);

        // previously used area is put into free pools
        if (!add_free_pool(data_ptr->id, data_ptr->off, h.padded_size)) {
          return NULL;
        }
      }
      return ptr;
    }

    virtual bool del(data_ptr_t *data_ptr)
    {
      // deleted chunk is put into free pools
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return false;
      }
      if (h.type == AREA_FREE) { return false; }

      if (!add_free_pool(data_ptr->id, data_ptr->off, h.padded_size)) {
        return false;
      }
      return true;
    }

    virtual data_t *get(data_ptr_t *data_ptr)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return false;
      }
      if (h.type == AREA_FREE) { return NULL; }

      data_t *data = new data_t;
      data->data = new char[h.size - sizeof(record_header_t)];
      data->size = h.size - sizeof(record_header_t);
      if (!_pread(fd_, (char *) data->data, data->size, 
                  off + sizeof(record_header_t))) {
        clean_data(data);
        return NULL;
      }
      return data;
    }

    // uses user allocated data
    // [TODO] arguments must be reconsidered. third argument size is kind of confusing.
    virtual bool get(data_ptr_t *data_ptr, data_t *data, uint32_t *size)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return false;
      }
      if (h.type == AREA_FREE) { return false; }

      *size = h.size - sizeof(record_header_t);
      if (data->size < *size) {
        ERROR_LOG("allocated size is too small for the data.");
        return false;
      }
      if (!_pread(fd_, (char *) data->data, *size, 
                  off + sizeof(record_header_t))) {
        return false;
      }
      return true;
    }

  private:

    record_t *init_record(data_t *data)
    {
      record_t *r = new record_t;
      record_header_t *h = new record_header_t;

      h->type = AREA_ALLOCATED;
      h->size = sizeof(record_header_t) + data->size;
      h->padded_size = get_padded_size(h->size);
      r->h = h;
      r->d = data;

      return r;
    }

    void clear_record(record_t *r)
    {
      delete r->h;
      delete r;
    }

    bool write_record(record_t *r, data_ptr_t *dp)
    {
      off_t off = calc_off(dp->id, dp->off);
      if (!write_header_and_data(r->h, r->d, off)) {
        return false;
      }
      return true;
    }
  };

  /*
   * Class LinkedData
   */
  class LinkedData : public Data {

#pragma pack(1)
    typedef struct {
      uint8_t type; // allocated or free
      uint32_t padded_size;
      block_id_t last_block_id;
      uint16_t last_off;
      uint8_t num_units;
    } record_header_t;

    // header of a succeeding record
    typedef struct {
      uint8_t type;
      uint32_t size;
      uint32_t padded_size; // size in a block
      block_id_t next_block_id;
      uint16_t next_off;
    } unit_header_t;
#pragma pack()

    typedef struct {
      unit_header_t *h;
      data_t *d;
    } unit_t;

    typedef struct {
      record_header_t *h;
      unit_t *u;
    } record_t;

  public:
    LinkedData(padding_mode_t pmode = PO2, uint32_t padding = DEFAULT_PADDING)
    : Data(Linked, pmode, padding)
    {}
    virtual ~LinkedData() {}

    virtual data_ptr_t *put(data_t *data)
    {
      record_t *r = init_record(data);

      data_ptr_t *data_ptr = alloc_space(r->h->padded_size);
      if (data_ptr == NULL) { return NULL; }
      if (!write_record(r, data_ptr)) {
        return NULL;
      }
  
      clear_unit(r->u);
      clear_record(r);

      return data_ptr;
    }

    virtual data_ptr_t *append(data_ptr_t *data_ptr, data_t *data)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return NULL;
      }
      if (h.type == AREA_FREE) { return NULL; }

      unit_header_t u;
      off_t last_off;
      if (h.num_units == 1) {
        // no succeeding units
        last_off = off + sizeof(record_header_t);
      } else {
        last_off = calc_off(h.last_block_id, h.last_off);
      }
      if (!_pread(fd_, &u, sizeof(unit_header_t), last_off)) {
        return NULL;
      }

      // if the padding is big enough, then no big deal. just put the data into it.
      uint32_t unused_size = u.padded_size - u.size;
      if (unused_size >= data->size) {
        if (!_pwrite(fd_, data->data, data->size, last_off + u.size)) {
          return NULL;
        }
        u.size += data->size;
      } else {
        if (h.num_units == UINT8_MAX) {
          ERROR_LOG("exceeds link limitation.");
          return NULL;
        }
        // write as much as possible into the padding
        if (!_pwrite(fd_, data->data, unused_size, last_off + u.size)) {
          return NULL;
        }

        // create a new unit and put the rest of the data into the unit
        data_t new_data = {(char *) data->data + unused_size, data->size - unused_size};
        unit_t *unit = init_unit(&new_data);
        uint32_t padding = get_padded_size(unit->h->size);
        unit->h->padded_size = padding > u.padded_size ? padding : u.padded_size;
        data_ptr_t *dp = put_unit(unit);

        // update unit header
        u.size = u.padded_size; // full
        u.next_block_id = dp->id;
        u.next_off = dp->off;

        // update record header
        ++(h.num_units);
        h.last_block_id = dp->id;
        h.last_off = dp->off;
        h.padded_size += unit->h->padded_size;
        if (!_pwrite(fd_, &h, sizeof(record_header_t), off)) {
          return NULL;
        }

        clear_unit(unit);
      }
      if (!_pwrite(fd_, &u, sizeof(unit_header_t), last_off)) {
        return NULL;
      }

      // pointer value unchanged in append in LinkedData 
      data_ptr_t *n_data_ptr = new data_ptr_t;
      memcpy(n_data_ptr, data_ptr, sizeof(data_ptr_t));

      return n_data_ptr;
    }

    virtual data_ptr_t *update(data_ptr_t *data_ptr, data_t *data)
    {
      data_ptr_t *ptr;
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return NULL;
      }
      if (h.type == AREA_FREE) { return NULL; }

      unit_header_t u;
      off += sizeof(record_header_t);
      if (!_pread(fd_, &u, sizeof(unit_header_t), off)) {
        return NULL;
      }

      // checking the first one only (not checking succeeding blocks)
      if (h.num_units == 1 && 
          u.padded_size - sizeof(unit_header_t) >= data->size) {
        // if the padding is big enough
        if (!_pwrite(fd_, data->data, data->size,
                     off + sizeof(unit_header_t))) {
          return NULL;
        }
        // update the header
        u.size = sizeof(unit_header_t) + data->size;
        if (!_pwrite(fd_, &u, sizeof(unit_header_t), off)) {
          return NULL;
        }
        // pointer unchanged
        ptr = new data_ptr_t;
        memcpy(ptr, data_ptr, sizeof(data_ptr_t));

      } else {
        // record moved, so pointing another place
        ptr = put(data);

        // previously used area is put into free pools
        del(data_ptr);
      }
      return ptr;
    }

    virtual bool del(data_ptr_t *data_ptr)
    {
      record_header_t h;
      off_t g_off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), g_off)) {
        return false; 
      }
      if (h.type == AREA_FREE) { return false; }

      int cnt = 0;
      block_id_t id = data_ptr->id;
      uint16_t off = data_ptr->off;
      g_off += sizeof(record_header_t);
      do {
        unit_header_t u;
        if (!_pread(fd_, &u, sizeof(unit_header_t), g_off)) {
          return false;
        }

        uint32_t size = u.padded_size;
        // for the first unit
        if (cnt == 0) {
          size += sizeof(record_header_t);
        }
        if (!add_free_pool(id, off, size)) { return false; }

        // next unit
        id = u.next_block_id;
        off = u.next_off; 
        g_off = calc_off(id, off);
      } while(++cnt < h.num_units);

      return true;
    }

    virtual data_t *get(data_ptr_t *data_ptr)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return NULL;
      }
      if (h.type == AREA_FREE) { return NULL; }

      data_t *data = new data_t;
      data->size = 0;
      char *d = new char[h.padded_size]; // must be more than data size
      char *p = d;

      int cnt = 0;
      off += sizeof(record_header_t);
      unit_header_t u;
      do {
        if (!_pread(fd_, &u, sizeof(unit_header_t), off)) {
          delete [] d; 
          return NULL;
        }

        off += sizeof(unit_header_t);
        if (!_pread(fd_, p, u.size - sizeof(unit_header_t), off)) {
          delete [] d; 
          return NULL;
        }
        p += u.size - sizeof(unit_header_t);
        data->size += u.size - sizeof(unit_header_t);

        // next unit
        off = calc_off(u.next_block_id, u.next_off); 
      } while (++cnt < h.num_units);

      data->data = d;
      return data;
    }

    virtual bool get(data_ptr_t *data_ptr, data_t *data, uint32_t *size)
    {
      record_header_t h;
      off_t off = calc_off(data_ptr->id, data_ptr->off);
      if (!_pread(fd_, &h, sizeof(record_header_t), off)) {
        return false;
      }

      if (h.type == AREA_FREE) { return false; }

      char *p = (char *) data->data;
      uint32_t data_size = 0;
      *size = 0;

      int cnt = 0;
      off += sizeof(record_header_t);
      unit_header_t u;
      do {
        if (!_pread(fd_, &u, sizeof(unit_header_t), off)) {
          return false;
        }

        data_size = u.size - sizeof(unit_header_t);
        *size += data_size;
        if (data->size < *size) {
          ERROR_LOG("allocated size is too small for the data");
          return false;
        }

        off += sizeof(unit_header_t);
        if (!_pread(fd_, p, data_size, off)) { return false; }
        p += data_size;

        // next unit
        off = calc_off(u.next_block_id, u.next_off); 
      } while (++cnt < h.num_units);

      return true;
    }

  private:
    record_t *init_record(data_t *data)
    {
      record_t *r = new record_t;
      record_header_t *h = new record_header_t;
      unit_t *u = init_unit(data);

      h->type = AREA_ALLOCATED;
      h->padded_size = sizeof(record_header_t) + u->h->padded_size;
      h->last_block_id = 0;
      h->last_off = 0;
      h->num_units = 1;

      r->h = h;
      r->u = u;

      return r;
    }

    void clear_record(record_t *r)
    {
      delete r->h;
      delete r;
    }

    unit_t *init_unit(data_t *data)
    {
      unit_t *u = new unit_t;
      unit_header_t *h = new unit_header_t;

      h->type = AREA_ALLOCATED;
      h->size = data->size + sizeof(unit_header_t);
      h->padded_size = get_padded_size(h->size);
      h->next_block_id = 0;
      h->next_off = 0;

      u->h = h;
      u->d = data;

      return u;
    }

    void clear_unit(unit_t *u)
    {
      delete u->h;
      delete u;
    }

    bool write_record(record_t *r, data_ptr_t *dp)
    {
      off_t off = calc_off(dp->id, dp->off);
      if (!_pwrite(fd_, r->h, sizeof(record_header_t), off)) {
        return false;
      }
      if (!write_header_and_data(r->u->h, r->u->d,
                                 off + sizeof(record_header_t))) {
        return false;
      }
      return true;
    }

    bool write_unit(unit_t *u, data_ptr_t *dp)
    {
      off_t off = calc_off(dp->id, dp->off);
      if (!write_header_and_data(u->h, u->d, off)) {
        return false;
      }
      return true;
    }

    data_ptr_t *put_unit(unit_t *u)
    {
      data_ptr_t *data_ptr = alloc_space(u->h->padded_size);
      if (!write_unit(u, data_ptr)) {
        return NULL;
      }
      return data_ptr;
    }
  };

}
}

#endif
