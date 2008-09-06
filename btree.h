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
    uint32_t node_num;
    uint16_t node_size;
    uint16_t init_data_size;
  } db_header_t;

  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t id;
    uint16_t key_num;
    uint16_t data_off;
    uint16_t free_off;
    uint16_t free_size;
    //uint32_t prev_id;
    //uint32_t next_id;
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
  typedef entry_t up_entry_t;

  typedef struct {
    uint16_t off;
    uint16_t size;
  } slot_t;

  typedef enum {
    KEY_FOUND,
    KEY_BIGGER,
    KEY_LESSER
  } find_key_t;

  typedef struct {
    char *data_p;
    char *slot_p;
    find_key_t type;
  } find_res_t;

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

      db_header_t dh;
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        // initialize the header for the newly created file
        //strncpy(h.magic, MAGIC, strlen(MAGIC));
        dh.key_num = 0;
        // one for db_header, one for root node and one for leaf node
        dh.node_num = 3;
        //dh.node_size = getpagesize();
        dh.node_size = 64;
        dh.init_data_size = dh.node_size - sizeof(node_header_t);

        if (_write(fd, &dh, sizeof(db_header_t)) < 0) {
          return false;
        }
        // [TODO] should fix because it might not work in BSD 4.3 ?
        if (ftruncate(fd, dh.node_size * dh.node_num) < 0) {
          return false;
        }
        map_ = (char *) mmap(0, dh.node_size * dh.node_num, 
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map_ == MAP_FAILED) {
          return false;
        }

        // root node and the first leaf node
        node_t *root = _init_node(1, dh.node_size, true, false);
        node_t *leaf = _init_node(2, dh.node_size, false, true);

        // make a link from root to the first leaf node
        memcpy(root->b, &(leaf->h->id), UI32_SIZE);
        root->h->data_off += UI32_SIZE;
        root->h->free_size -= UI32_SIZE;

        delete root;
        delete leaf;

      } else {
        if (_read(fd, &dh, sizeof(db_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        // [TODO] read filesize and compare with node_num * node_size
        // if they differ, gives alert and trust the filesize ?
        
        map_ = (char *) mmap(0, dh.node_size * dh.node_num,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);  
        if (map_ == MAP_FAILED) {
          std::cerr << "map failed" << std::endl;
          return false;
        }
      }

      fd_ = fd;
      oflags_ = oflags;
      dh_ = (db_header_t *) map_;

      std::cout << "key_num: " << dh_->key_num << std::endl;
      std::cout << "node_size: " << dh_->node_size << std::endl;
      std::cout << "node_num: " << dh_->node_num << std::endl;
      std::cout << "init_data_size: " << dh_->init_data_size << std::endl;
      std::cout << std::endl;

      node_t *root = _alloc_node(1);
      std::cout << "is_root: " << root->h->is_root << std::endl;
      std::cout << "is_leaf: " << root->h->is_leaf << std::endl;
      std::cout << "id: " << root->h->id << std::endl;
      std::cout << "key_num: " << root->h->key_num << std::endl;
      std::cout << "data_off: " << root->h->data_off << std::endl;
      std::cout << "free_off: " << root->h->free_off << std::endl;
      std::cout << std::endl;

      node_t *leaf = _alloc_node(2);
      std::cout << "is_root: " << leaf->h->is_root << std::endl;
      std::cout << "is_leaf: " << leaf->h->is_leaf << std::endl;
      std::cout << "id: " << leaf->h->id << std::endl;
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
    
      _insert(1, &entry, &up_entry);
    }

    void show_node(void)
    {
      for (int i = 1; i < dh_->node_num; ++i) {
        show_node(i);
      }
    }

    // debug method
    void show_node(uint32_t id)
    {
      std::cout << std::endl;
      std::cout << "========= SHOW NODE " << id << " ==========" << std::endl;
      node_t *node = _alloc_node(id);
      if (node == NULL) {
        std::cout << "node[ " << id << "] is not allocated, yet" << std::endl;
        return;
      }
      std::cout << "is_root: " << node->h->is_root << std::endl;
      std::cout << "is_leaf: " << node->h->is_leaf << std::endl;
      std::cout << "id: " << node->h->id << std::endl;
      std::cout << "key_num: " << node->h->key_num << std::endl;
      std::cout << "data_off: "<< node->h->data_off << std::endl;
      std::cout << "free_off: " << node->h->free_off << std::endl;
      std::cout << "free_size: " << node->h->free_size << std::endl;

      //if (node->h->is_leaf) {
        char *slot_p = (char *) node->h + dh_->node_size; // point to the tail of the node
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
        /*
      } else {
          std::cout << "non-leaf node !!!"<< std::endl;
      }
      */
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *dh_;

    node_t *_init_node(uint32_t id, uint16_t node_size, bool is_root, bool is_leaf)
    {
      char *node_p = (char *) &(map_[node_size * id]);
      node_header_t *node_hdr_p = (node_header_t *) node_p;
      node_hdr_p->is_root = is_root;
      node_hdr_p->is_leaf = is_leaf;
      node_hdr_p->id = id;
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

    node_t *_alloc_node(uint32_t id)
    {
      if (id > dh_->node_num - 1) {
        return NULL;
      }
      char *node_p = (char *) &(map_[dh_->node_size * id]);
      node_t *node = new node_t;
      node->h = (node_header_t *) node_p;
      node->b = (node_body_t *) (node_p + sizeof(node_header_t));
      return node;
    }

    void _insert(uint32_t id, entry_t *entry, up_entry_t **up_entry)
    {
      std::cerr << "insert: [" << id << "]" << std::endl;
      node_t *node = _alloc_node(id);
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
          node_t *new_node = _init_node(dh_->node_num-1, 
                                        dh_->node_size, false, true);
          //move_entries(node, new_node);
          split_leaf_node(node, new_node);

          // [TODO] to do something if the entry doesn't fit in the new node
          //put_entry_in_leaf(new_node, entry);

          // node is passed is for prefix key compression, which is not implemented yet.
          *up_entry = get_up_entry(node, new_node);

          return;
        }

      } else {
        uint32_t next_id = _find_next_node(node, entry);
        std::cout << "next_id: "<<  next_id << std::endl;
        _insert(next_id, entry, up_entry);

        if (*up_entry == NULL) { return; }

        if (node->h->free_size >= (*up_entry)->size + sizeof(slot_t)) {
          put_entry_in_nonleaf(node, *up_entry);
          up_entry_t *e = NULL;
          _insert(node->h->id, entry, &e);
        } else {
          std::cout << "non-leaf split is not implemented yet" << std::endl;
          // split non-leaf node
        }

        delete *up_entry;
        return;
      }
    }

    uint32_t _find_next_node(node_t *node, entry_t *entry)
    {
      std::cout << "find_next_node" << std::endl;
      std::cout << "id: " << node->h->id << std::endl;
      std::cout << "nkeys: " << node->h->key_num << std::endl;

      bool is_found = false;
      uint32_t next_id;
      char *data_p = (char *) node->b;
      char *slot_p = data_p + dh_->init_data_size;
      // [TODO] should specify ptr size by name
      memcpy(&next_id, (char *) node->b, UI32_SIZE);
      std::cout << "next_id: " << next_id << std::endl;

      if (node->h->key_num > 0) {
        for (int i = 0; i < node->h->key_num && !is_found; ++i) {
          slot_p -= sizeof(slot_t);
          slot_t *slot = (slot_t *) slot_p;
          char *key_buf = new char[slot->size+1];
          memcpy(key_buf, data_p + slot->off, slot->size);
          key_buf[slot->size] = '\0';
          std::cout << "!!! [" << key_buf << "]" << std::endl;

          // compare
          if (strcmp((char *) entry->key, key_buf) < 0) {
            std::cout << "FOUND!!" << std::endl;
            is_found = true;
          } else {
            // [TODO] 
            memcpy(&next_id, data_p + slot->off + slot->size, UI32_SIZE);
          }
          delete [] key_buf;
        }
      }
      std::cout << "next_id (last line in find_next_node): " << next_id << std::endl;
      return next_id;
    }

    void put_entry_in_leaf(node_t *node, entry_t *entry)
    {
      node_header_t *h = node->h;
      node_body_t *b = node->b;

      //char *data_p, *slot_p;
      //find_result_t res = find_entry_in_leaf(node, entry, &data_p, &slot_p);
      find_res_t *r = find_key(node, entry->key, entry->key_size);
      if (r->type == KEY_FOUND) {
        // update the value
        memcpy((char *) r->data_p + entry->key_size, entry->val, entry->val_size);
      } else {
        //_put_entry_in_leaf(node, entry, slot_p, res);
        //_put_entry_in_leaf(node, entry, r);
        put_entry(node, entry, r);
      }
      delete r;
    }

    void put_entry_in_nonleaf(node_t *node, entry_t *entry)
    {
      node_header_t *h = node->h;
      node_body_t *b = node->b;

      find_res_t *r = find_key(node, entry->key, entry->key_size);
      put_entry(node, entry, r);
      delete r;
    }
 
    // put_entry ?
    // identify if it's a leaf or not
    // if it's a up_entry, size of the value is sizeof(uint32_t) otherwise sizeof(uint32_t) + sizeof(uint16_t)
    //void _put_entry_in_leaf(node_t *node, entry_t *entry, char *slot_p, find_result_t res)
    void put_entry(node_t *node, entry_t *entry, find_res_t *r)
    {
      // append entry
      char *data_p = (char *) node->b + node->h->data_off;
      char *free_p = (char *) node->b + node->h->free_off;
      memcpy(data_p, entry->key, entry->key_size);
      memcpy(data_p + entry->key_size, entry->val, entry->val_size);

      // organize slots
      slot_t slot = { node->h->data_off, entry->key_size };
      if (r->type == KEY_LESSER) {
        // insert
        int shift_size = r->slot_p - free_p + sizeof(slot_t);
        std::cout << "shift_size: " << shift_size << std::endl;
        memmove(free_p - sizeof(slot_t), free_p, shift_size);
        memcpy(r->slot_p, &slot, sizeof(slot_t));
      } else {
        // prepend
        std::cout << "PREPEND" << std::endl;
        memcpy(free_p - sizeof(slot_t), &slot, sizeof(slot_t));
      }

      // update metadata
      node->h->data_off += entry->size;
      node->h->free_off -= sizeof(slot_t);
      node->h->free_size -= entry->size + sizeof(slot_t);
      ++(dh_->key_num);
      ++(node->h->key_num);
    }

    find_res_t *find_key(node_t *node, const void *key, uint32_t key_size)
    {
      char *data_p, *slot_p;
      find_res_t *r = new find_res_t;
      // [TODO] must be binar search, but linear search for now
      char *b = (char *) node->b;
      r->slot_p = (char *) node->b + dh_->init_data_size;
      for (int i = 1; i <= node->h->key_num; ++i) {
        r->slot_p -= sizeof(slot_t);
        slot_t *slot = (slot_t *) r->slot_p;

        char *key_buf = new char[slot->size + 1];   
        memset(key_buf, 0, slot->size + 1);
        memcpy(key_buf, b + slot->off, slot->size); 
        key_buf[slot->size] = '\0';

        // [TODO] key is regarded as a string
        int res = strcmp(key_buf, (char *) key);
        delete [] key_buf;

        if (res == 0) { // only comes in leaf
          r->data_p = b + slot->off;
          r->type = KEY_FOUND;
          return r;
        } else if (res > 0) {
          r->type = KEY_LESSER;
          return r;
        }
      }
      r->type = KEY_BIGGER;
      return r;
    }

    bool alloc_page(void)
    {
      ++(dh_->node_num);
      if (ftruncate(fd_, dh_->node_size * dh_->node_num) < 0) {
        return false;
      }
      // required ?
      //munmap(map_);
      map_ = (char *) mmap(0, dh_->node_size * dh_->node_num, 
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);  
      if (map_ == MAP_FAILED) {
        return false;
      }
      dh_ = (db_header_t *) map_;
      return true;
    }

    void split_leaf_node(node_t *node, node_t *new_node)
    {
      char *b = (char *) node->b;
      char *nb = (char *) new_node->b;
      node_header_t *h = node->h;
      node_header_t *nh = new_node->h;

      // current node slots
      slot_t *slots = (slot_t *) (b + h->free_off);

      // stay_num entries stay in the node, others move to the new node
      uint32_t moves = h->key_num / 2;
      uint32_t stays = h->key_num - moves;

      // copy the bigger entries to the new node
      uint16_t off = 0;
      char *slot_p = (char *) new_node->b + dh_->init_data_size;
      for (int i = moves - 1; i >= 0; --i) {
        // copy entry to the new node's data area
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + UI32_SIZE;
        memcpy(nb, b + (slots+i)->off, entry_size);
        nb += entry_size;
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        slot_p -= sizeof(slot_t);
        memcpy(slot_p, &slot, sizeof(slot_t));
        off += entry_size;
      }
      set_node_header(nh, off, moves);
      
      // copy staying entries into the buffers
      char dbuf[dh_->node_size], sbuf[dh_->node_size];
      char *dp = dbuf;
      char *sp = sbuf + dh_->node_size; // pointing the tail
      off = 0;
      for (int i = h->key_num - 1; i >= moves; --i) {
        // copy entry to the data buffer
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + UI32_SIZE;
        memcpy(dp, b + (slots+i)->off, entry_size);
        dp += entry_size;
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        sp -= sizeof(slot_t);
        memcpy(sp, &slot, sizeof(slot_t));
        off += entry_size;
      }

      // copy the buffers to the node
      slot_p = (char *) node->b + dh_->init_data_size;
      uint16_t slots_size = sizeof(slot_t) * stays;
      memcpy(b, dbuf, off); 
      memcpy(slot_p - slots_size, sp, slots_size);

      set_node_header(h, off, stays);
    }

    void set_node_header(node_header_t *h, uint16_t off, uint16_t nkeys)
    {
      h->data_off = off;
      h->free_off = dh_->init_data_size - nkeys * sizeof(slot_t);
      h->free_size = h->free_off - h->data_off;
      h->key_num = nkeys;
    }

    /*
     * copy entries and slots from a specified node to a buffer specified by dp and sp.
     * offsets in slots are modified along with entry move.
     */
    void copy_entry(char *dp, char *sp, node_t *node, slot_t *slots, int slot_from, int slot_to)
    {
      uint16_t off = 0;
      char *slot_p = (char *) node->b + dh_->init_data_size;
      for (int i = slot_from; i >= slot_to; --i) {
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + UI32_SIZE;
        memcpy(dp, (char *) node->b + (slots+i)->off, entry_size);
        dp += entry_size;
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        slot_p -= sizeof(slot_t);
        memcpy(slot_p, &slot, sizeof(slot_t));
        off += entry_size;
      }
    }

    up_entry_t *get_up_entry(node_t *left, node_t *right)
    {
      up_entry_t *up_entry = new up_entry_t;

      char *slot_p = (char *) right->b + dh_->init_data_size - sizeof(slot_t);
      slot_t *slot = (slot_t *) slot_p;
      up_entry->key = new char[slot->size];
      if (right->h->is_leaf) {
        memcpy((char *) up_entry->key, (char *) right->b, slot->size);
      } else {
        // there is a pointer before data
        memcpy((char *) up_entry->key, (char *) right->b + sizeof(uint32_t), slot->size);
      }
      up_entry->key_size = slot->size;
      up_entry->val_size = sizeof(uint32_t);
      up_entry->val = new char[sizeof(uint32_t)];
      // a value in up_entry is node id
      memcpy((char *) up_entry->val, &(right->h->id), up_entry->val_size);
      up_entry->size = up_entry->key_size + up_entry->val_size;
      //up_entry->id = right->h->id;
      return up_entry;
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
