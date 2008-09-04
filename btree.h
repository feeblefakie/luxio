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

#define UI16_SIZE sizeof(uint16_t)
#define UI32_SIZE sizeof(uint32_t)

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
    uint16_t data_off;
    uint16_t free_off;
    uint16_t free_size;
    //uint32_t prev_node_id;
    //uint32_t next_node_id;
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

  typedef struct {
    uint16_t off;
    uint16_t size;
  } slot_t;

  typedef enum {
    KEY_FOUND,
    KEY_BIGGER,
    KEY_LESSER
  } find_result_t;

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
        //db_hdr.node_size = getpagesize();
        db_hdr.node_size = 64;
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
        memcpy(root->b, &(leaf->h->node_id), UI32_SIZE);
        root->h->data_off += UI32_SIZE;

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
      std::cout << "data_off: " << root_->h->data_off << std::endl;
      std::cout << "free_off: " << root_->h->free_off << std::endl;
      std::cout << std::endl;

      node_t *leaf = _alloc_node(2);
      std::cout << "is_root: " << leaf->h->is_root << std::endl;
      std::cout << "is_leaf: " << leaf->h->is_leaf << std::endl;
      std::cout << "node_id: " << leaf->h->node_id << std::endl;
      std::cout << "key_num: " << leaf->h->key_num << std::endl;
      std::cout << "data_off: " << leaf->h->data_off << std::endl;
      std::cout << "free_off: " << leaf->h->free_off << std::endl;
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
      if (node == NULL) {
        std::cout << "node[ " << node_id << "] is not allocated, yet" << std::endl;
        return;
      }
      std::cout << "is_root: " << node->h->is_root << std::endl;
      std::cout << "is_leaf: " << node->h->is_leaf << std::endl;
      std::cout << "node_id: " << node->h->node_id << std::endl;
      std::cout << "key_num: " << node->h->key_num << std::endl;
      std::cout << "data_off: "<< node->h->data_off << std::endl;
      std::cout << "free_off: " << node->h->free_off << std::endl;
      std::cout << "free_size: " << node->h->free_size << std::endl;

      if (node->h->is_leaf) {
        char *slot_p = (char *) node->h + db_hdr_p_->node_size; // point to the tail of the node
        char *body_p = (char *) node->b;
        for (int i = 1; i <= node->h->key_num; ++i) {
          slot_p -= sizeof(slot_t);
          slot_t *slot = (slot_t *) slot_p;
          std::cout << "off[" << slot->off << "], size[" << slot->size << "]" << std::endl;

          char key_buf[256];
          uint32_t val;
          memset(key_buf, 0, 256);
          memcpy(key_buf, body_p + slot->off, slot->size);
          memcpy(&val, body_p + slot->off + slot->size, UI32_SIZE);
          std::cout << "key[" << key_buf << "]" << std::endl;
          std::cout << "val[" << val << "]" << std::endl;
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
      node_hdr_p->data_off = 0;
      node_hdr_p->free_off = node_size - sizeof(node_header_t);;
      node_hdr_p->free_size = node_hdr_p->free_off;
      node_body_t *node_body_p = (node_body_t *) (node_p + sizeof(node_header_t));

      node_t *node = new node_t;
      node->h = node_hdr_p;
      node->b = node_body_p;
      return node;
    }

    node_t *_alloc_node(uint32_t node_id)
    {
      if (node_id > db_hdr_p_->node_num - 1) {
        return NULL;
      }
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
        if (node->h->free_size >= entry->size + sizeof(slot_t)) {
          // there is enough space, then just put the entry
          put_entry_in_leaf(node, entry);
        } else {
          std::cout << "SPLITTING" << std::endl;
          if (!alloc_page()) {
            std::cerr << "alloc_page() failed" << std::endl; 
          }
          // create new leaf node
          node_t *new_node = _init_node(db_hdr_p_->node_num-1, 
                                        db_hdr_p_->node_size, false, true);
          //move_entries(node, new_node);
          split_leaf_node(node, new_node);

          // [TODO] to do something if the entry doesn't fit in the new node
          //put_entry_in_leaf(new_node, entry);

          // [TODO] take the most left entry(key) and new node's node_id
          // set in up_entry ?
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
      memcpy(&next_node_id, node->b, UI32_SIZE);
      data_p += UI32_SIZE;

      if (node->h->key_num > 0) {
        for (int i = 0; i < node->h->key_num && !is_found; ++i) {
          // get size and ptr from the slots
          uint16_t ptr, size;
          memcpy(&size, slot_p - UI16_SIZE, UI16_SIZE);
          memcpy(&ptr, slot_p - UI16_SIZE * 2, UI16_SIZE); 
          // get key
          char *key_buf = new char[size+1];
          memcpy(key_buf, data_p + ptr, size);
          key_buf[size] = '\0';

          // compare
          if (strcmp((char *) entry->key, key_buf) < 0) {
            is_found = true;
          } else {
            data_p += size;
            memcpy(&next_node_id, data_p, UI32_SIZE);
            data_p += UI32_SIZE;
          }
          delete [] key_buf;
        }
      }
      return next_node_id;
    }

    void put_entry_in_leaf(node_t *node, entry_t *entry)
    {
      node_header_t *h = node->h;
      node_body_t *b = node->b;

      char *data_p, *slot_p;
      find_result_t res = find_entry_in_leaf(node, entry, &data_p, &slot_p);
      if (res == KEY_FOUND) {
        // update the value
        memcpy((char *) data_p + entry->key_size, entry->val, entry->val_size);
      } else {
        _put_entry_in_leaf(node, entry, slot_p, res);
      }
    }
  
    void _put_entry_in_leaf(node_t *node, entry_t *entry, char *slot_p, find_result_t res)
    {
      // append entry
      char *data_p = (char *) node->b + node->h->data_off;
      char *free_p = (char *) node->b + node->h->free_off;
      memcpy(data_p, entry->key, entry->key_size);
      memcpy(data_p + entry->key_size, entry->val, entry->val_size);

      slot_t slot = { node->h->data_off, entry->key_size };
      if (res == KEY_LESSER) {
        // insert
        int shift_size = slot_p - free_p + sizeof(slot_t);
        std::cout << "shift_size: " << shift_size << std::endl;
        memmove(free_p - sizeof(slot_t), free_p, shift_size);
        memcpy(slot_p, &slot, sizeof(slot_t));
      } else {
        // prepend
        std::cout << "PREPEND" << std::endl;
        memcpy(free_p - sizeof(slot_t), &slot, sizeof(slot_t));
      }

      node->h->data_off += entry->size;
      node->h->free_off -= sizeof(slot_t);
      node->h->free_size -= entry->size + sizeof(slot_t);
      ++(db_hdr_p_->key_num);
      ++(node->h->key_num);
    }

    find_result_t find_entry_in_leaf(node_t *node, entry_t *entry, char **data_p, char **slot_p)
    {
      // [TODO] must be binar search, but linear search for now
      char *b = (char *) node->b;
      *slot_p = (char *) node->h + db_hdr_p_->node_size;
      for (int i = 1; i <= node->h->key_num; ++i) {
        *slot_p -= sizeof(slot_t);
        slot_t *slot = (slot_t *) *slot_p;

        char *key_buf = new char[slot->size + 1];   
        memset(key_buf, 0, slot->size + 1);
        memcpy(key_buf, b + slot->off, slot->size); 
        key_buf[slot->size] = '\0';

        // [TODO] entry->key is regarded as a string
        int res = strcmp(key_buf, (char *) entry->key);
        delete [] key_buf;

        if (res == 0) {
          *data_p = b + slot->off;
          return KEY_FOUND;
        } else if (res > 0) {
          return KEY_LESSER;
        }
      }
      return KEY_BIGGER;
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

    void split_leaf_node(node_t *node, node_t *new_node)
    {
      char *b = (char *) node->b;
      char *nb = (char *) new_node->b;

      // current node slots
      char *slot_p = (char *) b + node->h->free_off;
      slot_t **slots = (slot_t **) slot_p;
      for (int i = 0; i < node->h->key_num; ++i) {
        std::cout << "off: " << slots[i]->off << std::endl;
        std::cout << "size: " << slots[i]->size << std::endl;
      }
      slot_t *slot = (slot_t *) b + node->h->free_off;
      std::cout << "off: " << slot->off << std::endl;
      std::cout << "size: " << slot->size << std::endl;

      /*
      // stay_num entries stay in the node, others move to the new node
      uint32_t stay_num = node->h->key_num / 2;
      uint32_t move_num = node->h->key_num - stay_num;

      // copy the bigger entries to the new node
      uint16_t off = 0;
      char *slot_p = (char *) new_node->h + db_hdr_p_->node_size;
      for (int i = move_num - 1; i >= 0; --i) {
        // copy entry to the new node's data area
        // [TODO] value size is sizeof(uint32_t) for now
        memcpy(nb, b + slots[i]->off, slots[i]->size + UI32_SIZE);
        nb += slots[i]->size + UI32_SIZE;
        // new slot for the entry above
        slot_t slot = { off, slots[i]->size };
        memcpy(slot_p - sizeof(slot_t), &slot, sizeof(slot_t));
        slot_p -= sizeof(slot_t);
        // [TODO] value size is sizeof(uint32_t) for now
        off += slots[i]->size + UI32_SIZE;
      }
      // update header values
      new_node->h->data_off = off;
      new_node->h->free_off = db_hdr_p_->node_size - sizeof(node_header_t) - move_num * sizeof(slot_t);
      new_node->h->key_num = move_num;
      
      // copy staying entries into the buffers
      char data_buf[db_hdr_p_->node_size]; // buffer for the node's data
      char slot_buf[db_hdr_p_->node_size]; // buffer for the node's slot
      char *data_buf_p = data_buf;
      char *slot_buf_p = slot_buf + db_hdr_p_->node_size;
      off = 0;
      for (int i = node->h->key_num - 1; i >= move_num; --i) {
        // copy entry to the data buffer
        // [TODO] value size is sizeof(uint32_t) for now
        memcpy(data_buf_p, b + slots[i]->off, slots[i]->size + UI32_SIZE);
        data_buf_p += slots[i]->size + UI32_SIZE;
        // new slot for the entry above
        slot_t slot = { off, slots[i]->size };
        memcpy(slot_buf_p - sizeof(slot_t), &slot, sizeof(slot_t));
        slot_buf_p -= sizeof(slot_t);
        // [TODO] value size is sizeof(uint32_t) for now
        off += slots[i]->size + UI32_SIZE;
      }

      // copy the buffers to the node
      slot_p = (char *) node->h + db_hdr_p_->node_size;
      uint16_t slots_size = sizeof(slot_t) * stay_num;
      memcpy(b, data_buf, off); 
      memcpy(slot_p - slots_size, slot_buf_p, slots_size);
      // update header values
      node->h->data_off = off;
      node->h->free_off = db_hdr_p_->node_size - sizeof(node_header_t) - stay_num * sizeof(slot_t);
      node->h->key_num = stay_num;
      */
    }

/*
    void move_entries(node_t *node, node_t, *new_node)
    {
      uint32_t stay_entry_num = node->h->key_num / 2;
      uint32_t move_entry_num = node->h->key_num - stay_entry_num;

      char *slot_p = (char *) node->h + db_hdr_p_->node_size;
      slot_p -= UI16_SIZE * 2 * stay_entry_num;
      uint16_t ptr;
      memcpy(&ptr, slot_p - UI16_SIZE * 2, UI16_SIZE); 

      // move data
      memmove(new_node->b, node->b + ptr, node->h->data_off - ptr);
      new_node->h->data_off = node->h->data_off - ptr;

      // move slots
      uint16_t slot_size = UI16_SIZE * move_entry_num;
      memmove(new_node->b + new_node->h->free_off - slot_size,
              node->b + node->h->free_off, slot_size);
      new_node->h->free_off -= slot_size;

      // update key_num
      node->h->key_num = stay_entry_num;
      new_node->h->key_num = move_entry_num;

      // modify offset(ptr) values in slots
      uint16_t offset = 0;          
      char *tail_p = new_node->h + db_hdr_p_->node_size; // point to the tail of the node
      for (int i = 1; i <= new_node->h->key_num; ++i) {
        uint16_t size;
        //char *ptr_p = new_node->b + new_node->h->free_off - 2 * i * UI16_SIZE;
        char *ptr_p = tail_p - 2 * i * UI16_SIZE;
        char *size_p = size_p + UI16_SIZE;

        memcpy(ptr_p, offset, UI16_SIZE); // update offset
        memcpy(&size, size_p, UI16_SIZE);

        // [TODO] value in leaf node is currently uint32_t
        offset += size + UI32_SIZE;
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
