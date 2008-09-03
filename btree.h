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

  // global header
  typedef struct {
    uint32_t key_num;
    uint16_t node_size;
    uint32_t node_num;
  } db_header_t;

  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t node_id;
    uint16_t key_num;
    uint16_t data_ptr;
    uint16_t free_ptr;
    uint16_t free_size;
  } node_header_t;
  typedef char * node_body_t;

  typedef struct {
    node_header_t *h;
    node_body_t *b; 
  } node_t;

  typedef struct {
    const void *key;
    uint16_t key_size;
    const void *val;
    uint32_t val_size;
    uint32_t size;
  } entry_t;

  typedef struct {
    const void *key;
    uint16_t key_size;
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
        // [TODO] should fix because it might not work in BSD 4.3 ?
        if (ftruncate(fd, db_hdr.node_size * db_hdr.node_num) < 0) {
          return false;
        }
        map_ = (char *) mmap(0, db_hdr.node_size * db_hdr.node_num, 
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map_ == MAP_FAILED) {
          return false;
        }

        // root node and the first leaf node
        node_t *root = _init_node(1, db_hdr.node_size, true, false);
        node_t *leaf = _init_node(2, db_hdr.node_size, false, true);

        // make a link from root to the first leaf node
        memcpy(root->b, &(leaf->h->node_id), sizeof(uint32_t));
        root->h->data_ptr += sizeof(uint32_t);

        delete root;
        delete leaf;

      } else {
        if (_read(fd, &db_hdr, sizeof(db_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        // [TODO] read filesize and compare with node_num * node_size
        // if they differ, gives alert and trust the filesize ?
        
        map_ = (char *) mmap(0, db_hdr.node_size * db_hdr.node_num,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map_ == MAP_FAILED) {
          std::cerr << "map failed" << std::endl;
          return false;
        }
      }

      fd_ = fd;
      oflags_ = oflags;
      db_hdr_p_ = (db_header_t *) map_;

      std::cout << "key_num: " << db_hdr_p_->key_num << std::endl;
      std::cout << "node_size: " << db_hdr_p_->node_size << std::endl;
      std::cout << "node_num: " << db_hdr_p_->node_num << std::endl;
      std::cout << std::endl;

      root_ = _alloc_node(1);
      std::cout << "is_root: " << root_->h->is_root << std::endl;
      std::cout << "is_leaf: " << root_->h->is_leaf << std::endl;
      std::cout << "node_id: " << root_->h->node_id << std::endl;
      std::cout << "key_num: " << root_->h->key_num << std::endl;
      std::cout << "data_ptr: " << root_->h->data_ptr << std::endl;
      std::cout << "free_ptr: " << root_->h->free_ptr << std::endl;
      std::cout << std::endl;

      node_t *leaf = _alloc_node(2);
      std::cout << "is_root: " << leaf->h->is_root << std::endl;
      std::cout << "is_leaf: " << leaf->h->is_leaf << std::endl;
      std::cout << "node_id: " << leaf->h->node_id << std::endl;
      std::cout << "key_num: " << leaf->h->key_num << std::endl;
      std::cout << "data_ptr: " << leaf->h->data_ptr << std::endl;
      std::cout << "free_ptr: " << leaf->h->free_ptr << std::endl;
      delete leaf;
    }

    bool put(const void *key, uint32_t key_size, 
             const void *val, uint32_t val_size)
    {
      std::cout << "key: " << key << std::endl;
      std::cout << "key_size: " << key_size << std::endl;
      std::cout << "val: " << val << std::endl;
      std::cout << "val_size: " << val_size << std::endl;
      entry_t entry = {key, key_size, val, val_size, key_size + val_size};
      up_entry_t *up_entry = NULL;
    
      _insert(1, &entry, up_entry);
    }

    // debug method
    void show_node(uint32_t node_id)
    {
      std::cout << std::endl;
      std::cout << "========= SHOW NODE " << node_id << " ==========" << std::endl;
      node_t *node = _alloc_node(node_id);
      std::cout << "is_root: " << node->h->is_root << std::endl;
      std::cout << "is_leaf: " << node->h->is_leaf << std::endl;
      std::cout << "node_id: " << node->h->node_id << std::endl;
      std::cout << "key_num: " << node->h->key_num << std::endl;
      std::cout << "data_ptr: "<< node->h->data_ptr << std::endl;
      std::cout << "free_ptr: " << node->h->free_ptr << std::endl;
      std::cout << "free_size: " << node->h->free_size << std::endl;

      if (node->h->is_leaf) {
        char *tail_p = (char *) node->h + db_hdr_p_->node_size; // point to the tail of the node
        char *body_p = (char *) node->b;
        for (int i = 1; i <= node->h->key_num; ++i) {
          char *ptr_p = tail_p - 2 * i * sizeof(uint16_t);
          char *size_p = ptr_p + sizeof(uint16_t);
          uint16_t ptr, size;
          memcpy(&ptr, ptr_p, sizeof(uint16_t));
          memcpy(&size, size_p, sizeof(uint16_t));
          std::cout << "ptr[" << ptr << "], size[" << size << "]" << std::endl;

          char key_buf[256];
          uint32_t val;
          memset(key_buf, 0, 256);
          memcpy(key_buf, body_p, size);
          body_p += size;
          memcpy(&val, body_p, sizeof(uint32_t));
          body_p += sizeof(uint32_t);
          std::cout << "key[" << key_buf << "]" << std::endl;
          // [TODO] value in leaf node is currently uint32_t
          std::cout << "val[" << val << "]" << std::endl;
          // [TODO] value in leaf node is currently uint32_t
        }
      } else {
          std::cout << "non-leaf node !!!"<< std::endl;
      }
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *db_hdr_p_;
    node_t *root_;

    node_t *_init_node(uint32_t node_id, uint16_t node_size, bool is_root, bool is_leaf)
    {
      char *node_p = (char *) &(map_[node_size * node_id]);
      node_header_t *node_hdr_p = (node_header_t *) node_p;
      node_hdr_p->is_root = is_root;
      node_hdr_p->is_leaf = is_leaf;
      node_hdr_p->node_id = node_id;
      node_hdr_p->key_num = 0;
      node_hdr_p->data_ptr = 0;
      node_hdr_p->free_ptr = node_size - sizeof(node_header_t);;
      node_hdr_p->free_size = node_hdr_p->free_ptr;
      node_body_t *node_body_p = (node_body_t *) (node_p + sizeof(node_header_t));

      node_t *node = new node_t;
      node->h = node_hdr_p;
      node->b = node_body_p;
      return node;
    }

    node_t *_alloc_node(uint32_t node_id)
    {
      char *node_p = (char *) &(map_[db_hdr_p_->node_size * node_id]);
      node_t *node = new node_t;
      node->h = (node_header_t *) node_p;
      node->b = (node_body_t *) (node_p + sizeof(node_header_t));
      return node;
    }

    void _insert(uint32_t node_id, entry_t *entry, up_entry_t *up_entry)
    {
      std::cerr << "insert: [" << node_id << "]" << std::endl;
      node_t *node = _alloc_node(node_id);
      if (node->h->is_leaf) {
        if (node->h->free_size > entry->size) {
          // there is enough space, then just put the entry
          _put_entry(node, entry);
        } else {
          /*
          // no enough space, then splitting the node (codes below are tied up as split?)
          // allocate new page
          if (!alloc_page()) {
            std::cerr << "alloc_page() failed" << std::endl; 
          }
          // create new leaf node
          node_t *new_node = _init_node(db_hdr_p_->node_num-1, 
                                        db_hdr_p_->node_size, false, true);
          move_entries(node, new_node);

          // put the entry into the new node
          // [TODO] to do something if the entry doesn't fit in the new node
          _put_entry(new_node, entry);

          // [TODO] take the most left entry(key) and new node's node_id
          // set in up_entry ?

          */
        }

      } else {
        uint32_t next_node_id = _find_next_node(node, entry);
        _insert(next_node_id, entry, up_entry);

        if (up_entry == NULL) { return; }

        // up_entry is not NULL, then continues
        // [TODO] entry copied (or pushed) up
      } 
    }

    uint32_t _find_next_node(node_t *node, entry_t *entry)
    {
      std::cout << "find_next_node" << std::endl;
      bool is_found = false;
      uint32_t next_node_id;
      char *data_p = (char *) node->b;
      char *slot_p = (char *) node + db_hdr_p_->node_size;
      memcpy(&next_node_id, node->b, sizeof(uint32_t));
      data_p += sizeof(uint32_t);

      if (node->h->key_num > 0) {
        for (int i = 0; i < node->h->key_num && !is_found; ++i) {
          // get size and ptr from the slots
          uint16_t ptr, size;
          memcpy(&size, slot_p - sizeof(uint16_t), sizeof(uint16_t));
          memcpy(&ptr, slot_p - sizeof(uint16_t) * 2, sizeof(uint16_t)); 
          // get key
          char *key_buf = new char[size+1];
          memcpy(key_buf, data_p + ptr, size);
          key_buf[size] = '\0';

          // compare
          if (strcmp((char *) entry->key, key_buf) < 0) {
            is_found = true;
          } else {
            data_p += size;
            memcpy(&next_node_id, data_p, sizeof(uint32_t));
            data_p += sizeof(uint32_t);
          }
          delete [] key_buf;
        }
      }
      return next_node_id;
    }

    void _put_entry(node_t *node, entry_t *entry)
    {
      char buf[256];
      memset(buf, 0, 256);
      memcpy(buf, entry->key, entry->key_size);
      std::cerr << "put_entry: node_id[" << node->h->node_id << "]" 
                << " key[" << buf << "], size[" << entry->key_size << "]"
                << std::endl;
      node_header_t *h = node->h;
      node_body_t *b = node->b;

      // [TODO] put it in order (binary tree search)
      // just appending it currently
      uint16_t last_data_ptr = h->data_ptr;
      memcpy((char *) b + h->data_ptr, entry->key, entry->key_size);
      h->data_ptr += entry->key_size;
      memcpy((char *) b + h->data_ptr, entry->val, entry->val_size);
      h->data_ptr += entry->val_size;
      h->free_size -= entry->size; 

      // put slots
      memcpy((char *) b + h->free_ptr - sizeof(uint16_t), &(entry->key_size), sizeof(uint16_t));
      memcpy((char *) b + h->free_ptr - sizeof(uint16_t) * 2, &last_data_ptr, sizeof(uint16_t));
      h->free_ptr -= sizeof(uint16_t) * 2;
      h->free_size -= sizeof(uint16_t) * 2; 

      ++(db_hdr_p_->key_num);
      ++(node->h->key_num);
    }

    bool alloc_page(void)
    {
      ++(db_hdr_p_->node_num);
      if (ftruncate(fd_, db_hdr_p_->node_size * db_hdr_p_->node_num) < 0) {
        return false;
      }
      // required ?
      //munmap(map_);
      map_ = (char *) mmap(0, db_hdr_p_->node_size * db_hdr_p_->node_num, 
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);  
      if (map_ == MAP_FAILED) {
        return false;
      }
      return true;
    }
/*
    void move_entries(node_t *node, node_t, *new_node)
    {
      uint32_t stay_entry_num = node->h->key_num / 2;
      uint32_t move_entry_num = node->h->key_num - stay_entry_num;

      char *slot_p = (char *) node->h + db_hdr_p_->node_size;
      slot_p -= sizeof(uint16_t) * 2 * stay_entry_num;
      uint16_t ptr;
      memcpy(&ptr, slot_p - sizeof(uint16_t) * 2, sizeof(uint16_t)); 

      // move data
      memmove(new_node->b, node->b + ptr, node->h->data_ptr - ptr);
      new_node->h->data_ptr = node->h->data_ptr - ptr;

      // move slots
      uint16_t slot_size = sizeof(uint16_t) * move_entry_num;
      memmove(new_node->b + new_node->h->free_ptr - slot_size,
              node->b + node->h->free_ptr, slot_size);
      new_node->h->free_ptr -= slot_size;

      // update key_num
      node->h->key_num = stay_entry_num;
      new_node->h->key_num = move_entry_num;

      // modify offset(ptr) values in slots
      uint16_t offset = 0;          
      char *tail_p = new_node->h + db_hdr_p_->node_size; // point to the tail of the node
      for (int i = 1; i <= new_node->h->key_num; ++i) {
        uint16_t size;
        //char *ptr_p = new_node->b + new_node->h->free_ptr - 2 * i * sizeof(uint16_t);
        char *ptr_p = tail_p - 2 * i * sizeof(uint16_t);
        char *size_p = size_p + sizeof(uint16_t);

        memcpy(ptr_p, offset, sizeof(uint16_t)); // update offset
        memcpy(&size, size_p, sizeof(uint16_t));

        // [TODO] value in leaf node is currently uint32_t
        offset += size + sizeof(uint32_t);
      }
    }
    */

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
