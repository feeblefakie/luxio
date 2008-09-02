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

  typedef struct {
    uint32_t node_id;
    uint16_t key_num;
    uint16_t node_size;
    uint32_t node_num;
    uint16_t data_ptr;
    uint16_t free_ptr;
  } root_node_header_t;

  typedef struct {
    root_node_header_t h;
    char *data;
  } root_node_t;

  typedef struct {
    bool is_leaf;
    uint32_t node_id;
    uint16_t key_num;
    uint16_t data_ptr;
    uint16_t free_ptr;
  } node_header_t;

  typedef struct {
    node_header_t h;
    char *data;
  } node_t;


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
      root_node_header_t root_h;
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        // initialize the header for the newly created file
        //strncpy(h.magic, MAGIC, strlen(MAGIC));
        root_h.node_id = 0; // root node
        root_h.key_num = 0;
        root_h.node_size = getpagesize();
        root_h.node_num = 2; // one root node and one leaf node
        root_h.data_ptr = sizeof(root_node_header_t);
        root_h.free_ptr = root_h.node_size;

        if (_write(fd, &root_h, sizeof(root_node_header_t)) < 0) {
          return false;
        }
        // [TODO] should fix because it might not work in old BSD
        if (ftruncate(fd, root_h.node_size * root_h.node_num) < 0) {
          return false;
        }
        map = (char *) mmap(0, root_h.node_size * root_h.node_num, 
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map == MAP_FAILED) {
          return false;
        }

        // first leaf node
        node_t *node_ptr = (node_t *) &(map[root_h.node_size]);
        node_ptr->h.node_id = 1;
        node_ptr->h.key_num = 0;
        node_ptr->h.data_ptr = sizeof(node_header_t);
        node_ptr->h.free_ptr = root_h.node_size;

        // link to the first leaf node
        //memcpy(

      } else {
        if (_read(fd, &root_h, sizeof(root_node_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        map = (char *) mmap(0, root_h.node_size * root_h.node_num,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map == MAP_FAILED) {
          std::cerr << "map failed" << std::endl;
          return false;
        }
      }

      fd_ = fd;
      oflags_ = oflags;
      map_ = map;
      root_ptr_ = (root_node_t *) map;

      std::cout << root_ptr_->h.key_num << std::endl;
      std::cout << root_ptr_->h.node_num << std::endl;
      std::cout << root_ptr_->h.node_size << std::endl;

      node_t *node_ptr = (node_t *) &(map[root_ptr_->h.node_size]);

      std::cout << node_ptr->h.node_id << std::endl;
      std::cout << node_ptr->h.key_num << std::endl;
      std::cout << node_ptr->h.data_ptr << std::endl;
      std::cout << node_ptr->h.free_ptr << std::endl;
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    root_node_t *root_ptr_;
  
    //struct stat fstat_;

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
