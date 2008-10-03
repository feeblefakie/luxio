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

#ifndef LUX_DBM_ARRAY_H
#define LUX_DBM_ARRAY_H

#include "dbm.h"
#include "data.h"

#define ALLOC_AND_COPY(s1, s2, size) \
  char s1[size+1]; \
  memcpy(s1, s2, size); \
  s1[size] = '\0';

namespace Lux {
namespace DBM {

  const char *MAGIC = "LUXAR001";
  const int DEFAULT_PAGESIZE = getpagesize();

  typedef enum {
    NONCLUSTER,
    CLUSTER
  } db_index_t;

  typedef enum {
    OVERWRITE,
    NOOVERWRITE,
    APPEND // it's only supported in non-cluster index
  } insert_mode_t;

  // global header
  typedef struct {
    uint32_t num_keys;
    uint32_t num_pages;
    uint16_t page_size;
    uint8_t index_type;
    uint8_t data_size; // for fixed length value in cluster index
  } array_header_t;

  /*
   * Class Btree
   */
  class Array {
  public:
    Array(db_index_t index_type = NONCLUSTER,
          uint8_t data_size = sizeof(uint32_t))
    : index_type_(index_type),
      dt_(index_type == NONCLUSTER ? new LinkedData(NOPADDING) : NULL),
      data_size_(index_type == NONCLUSTER ? sizeof(data_ptr_t) : data_size)
    {}

    ~Array()
    {
      if (dt_ != NULL) {
        delete dt_;
        dt_ = NULL;
      }
      if (map_ != NULL) {
        close();
      }
    }

    bool open(std::string db_name, db_flags_t oflags)
    {
      std::string idx_db_name = db_name + ".aidx";
      fd_ = _open(idx_db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd_, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        return false;
      }

      array_header_t dh;
      memset(&dh, 0, sizeof(array_header_t));
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        dh.num_keys = 0;
        // one for db_header
        dh.num_pages = 1;
        dh.page_size = getpagesize();
        dh.index_type = index_type_;
        dh.data_size = data_size_;

        if (_write(fd_, &dh, sizeof(array_header_t)) < 0) {
          return false;
        }
        if (!alloc_pages(dh.num_pages, dh.page_size)) {
          std::cerr << "alloc_page failed in open" << std::endl;    
        }

      } else {
        if (_read(fd_, &dh, sizeof(array_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        // [TODO] read filesize and compare with num_nodes * node_size
        // if they differ, gives alert and trust the filesize ?
        map_ = (char *) _mmap(fd_, dh.page_size * dh.num_pages, oflags);
        if (map_ == NULL) { return false; }
      }

      oflags_ = oflags;
      dh_ = (array_header_t *) map_;
      allocated_size_ = dh_->page_size * dh_->num_pages;

      if (index_type_ != dh_->index_type) {
        std::cerr << "wrong index type" << std::endl;
        return false;
      }

      if (dh_->index_type == NONCLUSTER) {
        std::string data_db_name = db_name + ".data";
        dt_->open(data_db_name.c_str(), oflags);
      }
      return true;
    }

    bool close()
    {
      msync(map_, dh_->page_size * dh_->num_pages, MS_SYNC);
      munmap(map_, dh_->page_size * dh_->num_pages);
      map_ = NULL;
      ::close(fd_);
    }

    data_t *get(uint32_t index)
    {
      data_t *data;
      off_t off = index * dh_->data_size + dh_->page_size;

      if (off + dh_->data_size > allocated_size_) { return NULL; }

      if (dh_->index_type == CLUSTER) {
        data = new data_t;
        data->size = dh_->data_size;
        data->data = new char[dh_->data_size];
        memcpy((char *) data->data, map_ + off, dh_->data_size);
      } else {
        data_ptr_t data_ptr;
        memcpy(&data_ptr, map_ + off, sizeof(data_ptr_t));
        data = dt_->get(&data_ptr);
      }
      return data;
    }

    bool get(uint32_t index, data_t *data, uint32_t *size)
    {
      off_t off = index * dh_->data_size + dh_->page_size;

      if (off + dh_->data_size > allocated_size_) { return false; }

      if (dh_->index_type == CLUSTER) {
        if (data->size < dh_->data_size) {
          std::cerr << "allocated size is too small for the data" << std::endl;
          return false;
        }
        memcpy((char *) data->data, map_ + off, dh_->data_size);
        *size = dh_->data_size;
      } else {
        data_ptr_t data_ptr;
        memcpy(&data_ptr, map_ + off, sizeof(data_ptr_t));
        if (!dt_->get(&data_ptr, data, size)) {
          return false;
        }
      }
      return true;
    }

    bool clean_data(data_t *d)
    {
      delete [] (char *) (d->data);
      delete d;
    }

    bool put(uint32_t index,
              const void *val, uint32_t val_size, insert_mode_t flags = OVERWRITE)
    {
      data_t data = {val, val_size};
      return put(index, &data, flags);
    }

    bool put(uint32_t index, data_t *data, insert_mode_t flags = OVERWRITE)
    {
      off_t off = index * dh_->data_size + dh_->page_size;

      if (off + dh_->data_size > allocated_size_) {
        div_t d = div(off + dh_->data_size, dh_->page_size);
        uint32_t page_num = d.rem > 0 ? d.quot + 1 : d.quot;
        realloc_pages(page_num, dh_->page_size);
      }

      if (dh_->index_type == CLUSTER) {
        // only update is supported in cluster index
        memcpy(map_ + off, data->data, dh_->data_size);
      } else {
        data_ptr_t *res_data_ptr;
        data_ptr_t data_ptr;
        memcpy(&data_ptr, map_ + off, sizeof(data_ptr_t));

        if (data_ptr.id != 0 || data_ptr.off != 0) { // already stored
          if (flags == APPEND) {
            res_data_ptr = dt_->append(&data_ptr, data);
          } else { // OVERWRITE
            res_data_ptr = dt_->update(&data_ptr, data);
          }
        } else {
          res_data_ptr = dt_->put(data);
        }
        memcpy(map_ + off, res_data_ptr, sizeof(data_ptr_t));
        dt_->clean_data_ptr(res_data_ptr);
      }
      return true;
    }

    bool del(uint32_t index)
    {
    }

    void show_db_header()
    {
      std::cout << "========= SHOW DATABASE HEADER ==========" << std::endl;
      std::cout << "num_keys: " << dh_->num_keys << std::endl;
      std::cout << "num_nodes: " << dh_->num_pages << std::endl;
      std::cout << "node_size: " << dh_->page_size << std::endl;
      std::cout << "index_type: " << (int) dh_->index_type << std::endl;
      std::cout << "data_size: " << (int) dh_->data_size << std::endl;
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    array_header_t *dh_;
    db_index_t index_type_;
    uint8_t data_size_;
    uint64_t allocated_size_;
    Data *dt_;

    bool alloc_pages(uint32_t num_pages, uint16_t page_size)
    {
      allocated_size_ = page_size * num_pages;
      if (ftruncate(fd_, allocated_size_) < 0) {
        std::cerr << "ftruncate failed" << std::endl;
        return false;
      }
      map_ = (char *) _mmap(fd_, allocated_size_, oflags_);
      if (map_ == NULL) { return false; }

      return true;
    }

    bool realloc_pages(uint32_t num_pages, uint16_t page_size)
    {
      uint32_t prev_num_pages = dh_->num_pages; 

      if (map_ != NULL) {
        if (munmap(map_, dh_->page_size * dh_->num_pages) < 0) {
          std::cerr << "munmap failed" << std::endl;
          return false;
        }
      }
     
      if (!alloc_pages(num_pages, page_size)) {
        return false;
      }
      dh_ = (array_header_t *) map_;
      dh_->num_pages = num_pages;

      // fill zero in the newly allocated pages
      memset(map_ + dh_->page_size * prev_num_pages, 0, 
             dh_->page_size * (num_pages - prev_num_pages));

      return true;
    }

  };

}
}

#endif
