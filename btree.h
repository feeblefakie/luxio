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

#ifndef LIBMAP_BTREE_H
#define LIBMAP_BTREE_H

#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <iostream>
#include <string>

namespace LibMap {

  const char *MAGIC = "LMBT0001";
  const int DEFAULT_PAGESIZE = getpagesize();
  typedef int db_flags_t;
  const db_flags_t DB_RDONLY = 0x0000;
  const db_flags_t DB_RDWR = 0x0002;
  const db_flags_t DB_CREAT = 0x0200;
  const db_flags_t DB_TRUNC = 0x0400;
/*
  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t node_id;
    uint16_t key_num;
    uint16_t node_size;
    uint32_t node_num;
    uint16_t data_ptr;
    uint16_t free_ptr;
  } root_header_t;
  typedef char * root_body_t;
  */

  // global header
  typedef struct {
    uint32_t key_num;
    uint16_t node_size;
    uint32_t node_num;
  } db_header_t;

  typedef char * node_t;
  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t node_id;
    uint16_t key_num;
    uint16_t data_ptr;
    uint16_t free_ptr;
  } node_header_t;
  typedef char * node_body_t;

  typedef struct {
    const void *key;
    uint32_t key_size;
    const void *value;
    uint32_t value_size;
  } entry_t;

  typedef struct {
    const void *key;
    uint32_t key_size;
    uint32_t node_id;
  } up_entry_t;

  class Btree {
  public:
    Btree()
    {

    }
    ~Btree()
    {

    }
    bool open(std::string db_name, db_flags_t oflags)
    {
      int fd = -1;
      fd = _open(db_name.c_str(), oflags, 00644);
      if (fd < 0) {
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        return false;
      }

      char *map;
      db_header_t db_hdr;
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        // initialize the header for the newly created file
        //strncpy(h.magic, MAGIC, strlen(MAGIC));
        db_hdr.key_num = 0;
        db_hdr.node_size = getpagesize();
        // one for db_header, one for root node and one for leaf node
        db_hdr.node_num = 3;

        if (_write(fd, &db_hdr, sizeof(db_header_t)) < 0) {
          return false;
        }
        // [TODO] should fix because it might not work in old BSD
        if (ftruncate(fd, db_hdr.node_size * db_hdr.node_num) < 0) {
          return false;
        }
        map = (char *) mmap(0, db_hdr.node_size * db_hdr.node_num, 
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map == MAP_FAILED) {
          return false;
        }

        // must be private method (node_id is argument?)
        // root node (node id: 1)
        node_t *node_p = (node_t *) &(map[db_hdr.node_size*1]);
        node_header_t *node_hdr_p = (node_header_t *) node_p;
        node_hdr_p->is_root = true;
        node_hdr_p->is_leaf = false;
        node_hdr_p->node_id = 1;
        node_hdr_p->key_num = 0;
        node_hdr_p->data_ptr = sizeof(node_header_t);
        node_hdr_p->free_ptr = db_hdr.node_size;

        // first leaf node
        node_t *leaf_node_p = (node_t *) &(map[db_hdr.node_size*2]);
        node_header_t *leaf_node_hdr_p = (node_header_t *) leaf_node_p;
        leaf_node_hdr_p->is_root = false;
        leaf_node_hdr_p->is_leaf = true;
        leaf_node_hdr_p->node_id = 2;
        leaf_node_hdr_p->key_num = 0;
        leaf_node_hdr_p->data_ptr = sizeof(node_header_t);
        leaf_node_hdr_p->free_ptr = db_hdr.node_size;

        // must be inline method (linking from node_id to another node_id)
        // link to the first leaf node
        char *node_body_p = (char *) (node_p + sizeof(node_header_t));
        memcpy(node_body_p, &(leaf_node_hdr_p->node_id), sizeof(uint32_t));
        node_hdr_p->data_ptr += sizeof(uint32_t);

      } else {
        if (_read(fd, &db_hdr, sizeof(db_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        // [TODO] read filesize and compare with node_num * node_size
        // if they differ, gives alert and trust the filesize ?

        map = (char *) mmap(0, db_hdr.node_size * db_hdr.node_num,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map == MAP_FAILED) {
          std::cerr << "map failed" << std::endl;
          return false;
        }
      }

      fd_ = fd;
      oflags_ = oflags;
      map_ = map;
      db_hdr_p_ = (db_header_t *) map;

      std::cout << "key_num: " << db_hdr_p_->key_num << std::endl;
      std::cout << "node_size: " << db_hdr_p_->node_size << std::endl;
      std::cout << "node_num: " << db_hdr_p_->node_num << std::endl;
      std::cout << std::endl;

      root_hdr_p_ = (node_header_t *) &(map[db_hdr_p_->node_size*1]);
      root_body_p_ = (node_body_t *) (map + sizeof(node_header_t));

      std::cout << "is_root: " << root_hdr_p_->is_root << std::endl;
      std::cout << "is_leaf: " << root_hdr_p_->is_leaf << std::endl;
      std::cout << "node_id: " << root_hdr_p_->node_id << std::endl;
      std::cout << "key_num: " << root_hdr_p_->key_num << std::endl;
      std::cout << "data_ptr: " << root_hdr_p_->data_ptr << std::endl;
      std::cout << "free_ptr: " << root_hdr_p_->free_ptr << std::endl;
      std::cout << std::endl;

      node_header_t *node_hdr_p = (node_header_t *) &(map[db_hdr_p_->node_size*2]);

      std::cout << "is_root: " << node_hdr_p->is_root << std::endl;
      std::cout << "is_leaf: " << node_hdr_p->is_leaf << std::endl;
      std::cout << "node_id: " << node_hdr_p->node_id << std::endl;
      std::cout << "key_num: " << node_hdr_p->key_num << std::endl;
      std::cout << "data_ptr: " << node_hdr_p->data_ptr << std::endl;
      std::cout << "free_ptr: " << node_hdr_p->free_ptr << std::endl;
    }

    bool put(const void *key, uint32_t key_size, 
             const void *value, uint32_t value_size)
    {
      entry_t entry = {key, key_size, value, value_size};
      up_entry_t *up_entry = NULL;
      //_insert(0, &entry, up_entry);
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *db_hdr_p_;
    node_header_t *root_hdr_p_;
    node_body_t *root_body_p_;

    bool _insert(uint32_t node_id, entry_t *entry, up_entry_t *up_entry)
    {
      &(map_[node_id]);

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
