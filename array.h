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
#include <pthread.h>

namespace Lux {
namespace DBM {

  const char *MAGIC = "LUXAR001";
  const int DEFAULT_PAGESIZE = getpagesize();

  // global header
  typedef struct {
    uint32_t num_keys;
    uint32_t num_pages;
    uint16_t page_size;
    uint8_t index_type;
    uint8_t data_size; // for fixed length value in cluster index
    uint32_t num_resized;
  } array_header_t;

  /*
   * Class Btree
   */
  class Array {
  public:
    Array(db_index_t index_type = CLUSTER,
          uint8_t data_size = sizeof(uint32_t))
    : map_(NULL),
      dt_(NULL),
      smode_(Padded),
      pmode_(RATIO),
      padding_(20),
      index_type_(index_type),
      data_size_(index_type == NONCLUSTER ? sizeof(data_ptr_t) : data_size)
    {
      if (pthread_rwlock_init(&rwlock_, NULL) != 0) {
        ERROR_LOG("pthread_rwlock_init failed.");
        exit(-1);
      }
    }

    ~Array()
    {
      if (dt_ != NULL) {
        delete dt_;
        dt_ = NULL;
      }
      if (map_ != NULL) {
        close();
      }
      if (pthread_rwlock_destroy(&rwlock_) != 0) {
        ERROR_LOG("pthread_rwlock_destroy failed.");
        exit(-1);
      }
    }

    bool open(std::string db_name, db_flags_t oflags)
    {
      if (lock_type_ == LOCK_THREAD) {
        pthread_rwlock_wrlock(&rwlock_);
      }
      bool res = open_(db_name, oflags);
      if (lock_type_ == LOCK_THREAD) {
        pthread_rwlock_unlock(&rwlock_);
      }

      return res;
    }

    bool close()
    {
      wlock_db();
      msync(map_, dh_->page_size * dh_->num_pages, MS_SYNC);
      munmap(map_, dh_->page_size * dh_->num_pages);
      map_ = NULL;
      ::close(fd_);
      unlock_db();

      return true;
    }

    data_t *get(uint32_t index)
    {
      data_t *data;
      
      rlock_db();
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
      unlock_db();
      return data;
    }

    bool get(uint32_t index, data_t *data, uint32_t *size)
    {
      rlock_db();
      off_t off = index * dh_->data_size + dh_->page_size;
      if (off + dh_->data_size > allocated_size_) { return false; }

      if (dh_->index_type == CLUSTER) {
        if (data->size < dh_->data_size) {
          ERROR_LOG("allocated size is too small for the data.");
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
      unlock_db();
      return true;
    }

    bool put(uint32_t index,
             const void *val, uint32_t val_size, insert_mode_t flags = OVERWRITE)
    {
      data_t data = {val, val_size};
      return put(index, &data, flags);
    }

    bool put(uint32_t index, data_t *data, insert_mode_t flags = OVERWRITE)
    {
      wlock_db();
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
      unlock_db();
      return true;
    }

    bool del(uint32_t index)
    {
      wlock_db();
      off_t off = index * dh_->data_size + dh_->page_size;
      if (off + dh_->data_size > allocated_size_) { return false; }

      if (dh_->index_type == CLUSTER) {
        // [NOTICE] deleting only fills zero
        memset(map_ + off, 0, dh_->data_size);
      } else {
        data_ptr_t data_ptr;
        memcpy(&data_ptr, map_ + off, sizeof(data_ptr_t));
        dt_->del(&data_ptr);
      }
      unlock_db();
      return true;
    }

    bool set_lock_type(lock_type_t lock_type)
    {
      lock_type_ = lock_type;
    }

    // only for noncluster database
    bool set_noncluster_params(store_mode_t smode,
                               padding_mode_t pmode = RATIO, uint32_t padding = 20)
    {
      smode_ = smode;
      pmode_ = pmode;
      padding_ = padding;
    }

    bool clean_data(data_t *d)
    {
      delete [] (char *) (d->data);
      delete d;
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
    store_mode_t smode_;
    padding_mode_t pmode_;
    uint32_t padding_;
    pthread_rwlock_t rwlock_;
    uint32_t num_pages_;
    uint16_t page_size_;
    uint32_t num_resized_;
    lock_type_t lock_type_;

    bool open_(std::string db_name, db_flags_t oflags)
    {
      std::string idx_db_name = db_name + ".aidx";
      fd_ = _open(idx_db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        return false;
      }
      oflags_ = oflags;
      if (lock_type_ == LOCK_PROCESS) {
        if (flock(fd_, LOCK_EX) != 0) { 
          ERROR_LOG("flock failed.");
          return false;
        }
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
        dh.num_resized = 0;
        dh.index_type = index_type_;
        dh.data_size = data_size_;

        if (_write(fd_, &dh, sizeof(array_header_t)) < 0) {
          return false;
        }
        if (!alloc_pages(dh.num_pages, dh.page_size)) {
          ERROR_LOG("alloc_page failed.");
        }

      } else {
        if (_read(fd_, &dh, sizeof(array_header_t)) < 0) {
          ERROR_LOG("read failed");
          return false;
        }

        // [TODO] read filesize and compare with num_nodes * node_size
        // if they differ, gives alert and trust the filesize ?
        map_ = (char *) _mmap(fd_, dh.page_size * dh.num_pages, oflags);
        if (map_ == NULL) { return false; }
      }

      dh_ = (array_header_t *) map_;
      allocated_size_ = dh_->page_size * dh_->num_pages;
      num_pages_ = dh_->num_pages;
      page_size_ = dh_->page_size;
      num_resized_ = dh_->num_resized;


      if (index_type_ != dh_->index_type) {
        ERROR_LOG("wrong index type");
        return false;
      }

      if (dh_->index_type == NONCLUSTER) {
        if(smode_ == Padded) {
          dt_ = new PaddedData(pmode_, padding_);
        } else {
          dt_ = new LinkedData(pmode_, padding_);
        }
        std::string data_db_name = db_name + ".data";
        dt_->open(data_db_name.c_str(), oflags);
      }

      if (lock_type_ == LOCK_PROCESS) {
        if (flock(fd_, LOCK_UN) != 0) { return false; }
      }

      return true;
    }

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
          std::cerr << "munmap failed in realloc_pages" << std::endl;
          return false;
        }
      }
     
      if (!alloc_pages(num_pages, page_size)) {
        return false;
      }
      dh_ = (array_header_t *) map_;
      dh_->num_pages = num_pages;
      ++(dh_->num_resized);
      num_pages_ = num_pages;

      // fill zero in the newly allocated pages
      memset(map_ + dh_->page_size * prev_num_pages, 0, 
             dh_->page_size * (num_pages - prev_num_pages));

      return true;
    }

    bool remap(void)
    {
      uint32_t num_pages = dh_->num_pages;
      if (munmap(map_, page_size_ * num_pages_) < 0) {
        std::cerr << "munmap failed in remap" << std::endl;
        return false;
      }
      map_ = (char *) _mmap(fd_, page_size_ * num_pages, oflags_);
      if (map_ == NULL) {
        std::cerr << "map failed" << std::endl;
        return false;
      }
      dh_ = (array_header_t *) map_;
      num_pages_ = dh_->num_pages;
      num_resized_ = dh_->num_resized;

      return true;
    }

    bool unlock_db(void)
    {
      if (lock_type_ == NO_LOCK) {
        return true;
      } else if (lock_type_ == LOCK_THREAD) {
        // thread level locking
        pthread_rwlock_unlock(&rwlock_);
      } else {
        // process level locking
        if (flock(fd_, LOCK_UN) != 0) { return false; }
      }
    }

    bool rlock_db(void)
    {
      if (lock_type_ == NO_LOCK) {
        return true;
      } else if (lock_type_ == LOCK_THREAD) {
        // thread level locking
        pthread_rwlock_rdlock(&rwlock_);
      } else {
        // process level locking
        if (flock(fd_, LOCK_SH) != 0) { 
          std::cerr << "flock failed in get" << std::endl;
          return false;
        }
        if (num_resized_ != dh_->num_resized) {
          remap();
        }
      }
    }

    bool wlock_db(void)
    {
      if (lock_type_ == NO_LOCK) {
        return true;
      } else if (lock_type_ == LOCK_THREAD) {
        // thread level locking
        pthread_rwlock_wrlock(&rwlock_);
      } else {
        // process level locking
        if (flock(fd_, LOCK_EX) != 0) { 
          std::cerr << "flock failed in get" << std::endl;
          return false;
        }
        if (num_resized_ != dh_->num_resized) {
          remap();
        }
      }
    }
  };

}
}

#endif
