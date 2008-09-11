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
#include <stdlib.h>
#include <iostream>
#include <string>

#define ALLOC_AND_COPY(s1, s2, size) \
  char s1[size+1]; \
  memcpy(s1, s2, size); \
  s1[size] = '\0';

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
    uint32_t num_keys;
    uint32_t num_nodes;
    uint16_t node_size;
    uint16_t init_data_size;
    uint32_t root_id;
  } db_header_t;

  typedef uint32_t node_id_t;
  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t id;
    uint16_t num_keys;
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
    KEY_SMALLEST,
    KEY_BIGGEST
  } find_key_t;

  typedef struct {
    char *data_p;
    char *slot_p;
    find_key_t type;
  } find_res_t;

  typedef struct {
    const void *data;
    uint32_t size;
  } data_t;

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
      fd_ = _open(db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd_, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        return false;
      }

      db_header_t dh;
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        // initialize the header for the newly created file
        //strncpy(h.magic, MAGIC, strlen(MAGIC));
        dh.num_keys = 0;
        // one for db_header, one for root node and one for leaf node
        dh.num_nodes = 3;
        dh.node_size = getpagesize();
        //dh.node_size = 64;
        dh.init_data_size = dh.node_size - sizeof(node_header_t);
        dh.root_id = 1;

        if (_write(fd_, &dh, sizeof(db_header_t)) < 0) {
          return false;
        }
        if (!alloc_page(dh.num_nodes, dh.node_size)) {
          std::cerr << "alloc_page failed in open" << std::endl;    
        }

        // root node and the first leaf node
        node_t *root = _init_node(1, true, false);
        node_t *leaf = _init_node(2, false, true);

        // make a link from root to the first leaf node
        memcpy(root->b, &(leaf->h->id), sizeof(node_id_t));
        root->h->data_off += sizeof(node_id_t);
        root->h->free_size -= sizeof(node_id_t);

        delete root;
        delete leaf;

      } else {
        if (_read(fd_, &dh, sizeof(db_header_t)) < 0) {
          std::cerr << "read failed" << std::endl;
          return false;
        }

        // [TODO] read filesize and compare with num_nodes * node_size
        // if they differ, gives alert and trust the filesize ?
        
        map_ = (char *) mmap(0, dh.node_size * dh.num_nodes,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);  
        if (map_ == MAP_FAILED) {
          std::cerr << "map failed" << std::endl;
          return false;
        }
      }

      oflags_ = oflags;
      dh_ = (db_header_t *) map_;
    }

    bool close()
    {
      //msync(map_, dh_->node_size * dh_->num_nodes, MS_ASYNC);
      munmap(map_, dh_->node_size * dh_->num_nodes);
      ::close(fd_);
    }

    data_t *get(const void *key, uint32_t key_size)
    {
      data_t key_data = {key, key_size};
      data_t *val_data = NULL;
      find(dh_->root_id, &key_data, &val_data);
      return val_data;
    }

    bool clean_data(data_t *d)
    {
      delete [] (char *) (d->data);
      delete d;
    }

    bool put(const void *key, uint32_t key_size, const void *val, uint32_t val_size)
    {
      entry_t entry = {(char *) key, key_size,
                       (char *) val, val_size, key_size + val_size};
      up_entry_t *up_entry = NULL;
    
      insert(dh_->root_id, &entry, &up_entry);
    }

    void show_node(void)
    {
      show_db_header();
      for (int i = 1; i < dh_->num_nodes; ++i) {
        show_node(i);
      }
    }

    void show_db_header()
    {
      std::cout << "========= SHOW ROOT ==========" << std::endl;
      std::cout << "num_keys: " << dh_->num_keys << std::endl;
      std::cout << "num_nodes: " << dh_->num_nodes << std::endl;
      std::cout << "node_size: " << dh_->node_size << std::endl;
      std::cout << "init_data_size: " << dh_->init_data_size << std::endl;
      std::cout << "root_id: " << dh_->root_id << std::endl;
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
      std::cout << "num_keys: " << node->h->num_keys << std::endl;
      std::cout << "data_off: "<< node->h->data_off << std::endl;
      std::cout << "free_off: " << node->h->free_off << std::endl;
      std::cout << "free_size: " << node->h->free_size << std::endl;

      if (!node->h->is_leaf) {
        node_id_t leftmost; 
        memcpy(&leftmost, (char *) node->b, sizeof(node_id_t));
        std::cout << "leftmost[" << leftmost << "]" << std::endl;
      }

      char *slot_p = (char *) node->h + dh_->node_size; // point to the tail of the node
      char *body_p = (char *) node->b;
      for (int i = 1; i <= node->h->num_keys; ++i) {
        slot_p -= sizeof(slot_t);
        slot_t *slot = (slot_t *) slot_p;
        std::cout << "off[" << slot->off << "], size[" << slot->size << "]" << std::endl;

        char key_buf[256];
        uint32_t val;
        memset(key_buf, 0, 256);
        memcpy(key_buf, body_p + slot->off, slot->size);
        memcpy(&val, body_p + slot->off + slot->size, sizeof(node_id_t));
        std::cout << "key[" << key_buf << "]" << std::endl;
        std::cout << "val[" << val << "]" << std::endl;
      }
    }

  private:
    int fd_;
    db_flags_t oflags_;
    char *map_;
    db_header_t *dh_;

    node_t *_init_node(uint32_t id, bool is_root, bool is_leaf)
    {
      char *node_p = (char *) &(map_[dh_->node_size * id]);
      node_header_t *node_hdr_p = (node_header_t *) node_p;
      node_hdr_p->is_root = is_root;
      node_hdr_p->is_leaf = is_leaf;
      node_hdr_p->id = id;
      node_hdr_p->num_keys = 0;
      node_hdr_p->data_off = 0;
      node_hdr_p->free_off = dh_->node_size - sizeof(node_header_t);;
      node_hdr_p->free_size = node_hdr_p->free_off;
      node_body_t *node_body_p = (node_body_t *) (node_p + sizeof(node_header_t));

      node_t *node = new node_t;
      node->h = node_hdr_p;
      node->b = node_body_p;
      return node;
    }

    node_t *_alloc_node(uint32_t id)
    {
      if (id > dh_->num_nodes - 1) {
        return NULL;
      }
      char *node_p = (char *) &(map_[dh_->node_size * id]);
      node_t *node = new node_t;
      node->h = (node_header_t *) node_p;
      node->b = (node_body_t *) (node_p + sizeof(node_header_t));
      return node;
    }

    bool find(node_id_t id, data_t *key_data, data_t **val_data)
    {
      entry_t entry = {key_data->data, key_data->size, NULL, 0, 0};
      entry_t *e = &entry;

      node_t *node = _alloc_node(id);
      if (node->h->is_leaf) {
        find_res_t *r = find_key(node, entry.key, entry.key_size);
        if (r->type == KEY_FOUND) {
          slot_t *slot = (slot_t *) r->slot_p;
          *val_data = new data_t;
          (*val_data)->data = (char *) new char[slot->size+1];
          // [TODO] data size is sizeof(uint32_t) for now 
          (*val_data)->size = sizeof(uint32_t);
          memcpy((void *) (*val_data)->data, (const char *) r->data_p + slot->size, sizeof(uint32_t));
        }
        delete r;
      } else {
        uint32_t next_id = _find_next(node, &entry);
        find(next_id, key_data, val_data);
      }
      delete node;
    }

    bool insert(node_id_t id, entry_t *entry, up_entry_t **up_entry)
    {
      bool is_split = false;

      _insert(dh_->root_id, entry, up_entry, is_split);

      if (is_split) {
        up_entry_t *e = NULL;
        bool is_split = false;
        _insert(dh_->root_id, entry, &e, is_split);
        if (is_split) {
          // try couple of times (not forever)
          return false;
        }
      }
      return true;
    }

    void _insert(node_id_t id, entry_t *entry, up_entry_t **up_entry, bool &is_split)
    {
      node_t *node = _alloc_node(id);
      if (node->h->is_leaf) {
        if (node->h->free_size >= entry->size + sizeof(slot_t)) {
          // there is enough space, then just put the entry
          put_entry_in_leaf(node, entry);
        } else {
          if (update_if_exists(node, entry)) { return; }

          if (!append_page()) {
            std::cerr << "alloc_page() failed" << std::endl; 
          }
          // must reallocate after remapped
          delete node;
          node = _alloc_node(id);

          // create new leaf node
          node_t *new_node = _init_node(dh_->num_nodes-1, false, true);
          split_node(node, new_node, up_entry);
          delete new_node;
          is_split = true;
        }
      } else {
        node_id_t next_id = _find_next(node, entry);
        _insert(next_id, entry, up_entry, is_split);

        if (*up_entry == NULL) { return; }

        // must reallocate after remapped
        delete node;
        node = _alloc_node(id);

        if (node->h->free_size >= (*up_entry)->size + sizeof(slot_t)) {
          put_entry_in_nonleaf(node, *up_entry);
          clean_up_entry(up_entry);
        } else {
          if (!append_page()) {
            std::cerr << "alloc_page() failed" << std::endl; 
          }
          // must reallocate after remapped
          delete node;
          node = _alloc_node(id);

          // pointing the pushed up entry
          up_entry_t *e = *up_entry;
          *up_entry = NULL;

          node_t *new_node = _init_node(dh_->num_nodes-1, false, false);
          split_node(node, new_node, up_entry);

          // compare e with up_e to decide which node putting e into
          ALLOC_AND_COPY(e_key, e->key, e->key_size);
          ALLOC_AND_COPY(up_e_key, (*up_entry)->key, (*up_entry)->key_size);

          if (strcmp(e_key, up_e_key) < 0) {
            put_entry_in_nonleaf(node, e); // goes to old node
          } else {
            put_entry_in_nonleaf(new_node, e); // goes to new node
          }
          delete new_node;

          if (node->h->is_root) {
            if (!append_page()) {
              std::cerr << "alloc_page() failed" << std::endl; 
            }
            delete node;
            node = _alloc_node(id);
            
            node_t *new_root = _init_node(dh_->num_nodes-1, true, false);
            make_leftmost_ptr(new_root, (char *) &(node->h->id));
            put_entry_in_nonleaf(new_root, *up_entry);
            delete new_root;
            // change root 
            dh_->root_id = new_root->h->id;
            node->h->is_root = false;
          }
          clean_up_entry(&e);
        }
      }
      delete node;
    }

    node_id_t _find_next(node_t *node, entry_t *entry)
    {
      node_id_t id;
      find_res_t *r = find_key(node, entry->key, entry->key_size);
      if (r->type == KEY_SMALLEST) {
          memcpy(&id, (char *) node->b, sizeof(node_id_t));
      } else {
        slot_t *slot = (slot_t *) r->slot_p;
        memcpy(&id, (char *) node->b + slot->off + slot->size, sizeof(node_id_t));
      }
      return id;
    }

    bool update_if_exists(node_t *node, entry_t *entry)
    {
      find_res_t *r = find_key(node, entry->key, entry->key_size);
      if (r->type == KEY_FOUND) {
        // update the value
        memcpy((char *) r->data_p + entry->key_size, entry->val, entry->val_size);
        delete r;
        return true;
      }
      return false;
    }

    void put_entry_in_leaf(node_t *node, entry_t *entry)
    {
      node_header_t *h = node->h;
      node_body_t *b = node->b;

      find_res_t *r = find_key(node, entry->key, entry->key_size);
      if (r->type == KEY_FOUND) {
        // update the value
        memcpy((char *) r->data_p + entry->key_size, entry->val, entry->val_size);
      } else {
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
 
    void put_entry(node_t *node, entry_t *entry, find_res_t *r)
    {
      // append entry
      char *data_p = (char *) node->b + node->h->data_off;
      char *free_p = (char *) node->b + node->h->free_off;
      memcpy(data_p, entry->key, entry->key_size);
      memcpy(data_p + entry->key_size, entry->val, entry->val_size);

      // organize ordered slots
      slot_t slot = { node->h->data_off, entry->key_size };

      if (r->type == KEY_BIGGEST ||
          (r->type == KEY_SMALLEST && node->h->num_keys == 0)) {
        // prepend
        memcpy(free_p - sizeof(slot_t), &slot, sizeof(slot_t));
      } else if (r->type == KEY_SMALLEST) {
        // insert:KEY_SMALLEST (shifting all the slots)
        char *tail_p = (char *) node->b + dh_->init_data_size;
        int shift_size = tail_p - free_p;
        memmove(free_p - sizeof(slot_t), free_p, shift_size);
        memcpy(tail_p - sizeof(slot_t), &slot, sizeof(slot_t));
      } else {
        // insert:KEY_BIGGER (shifting some of the slots)
        int shift_size = r->slot_p - free_p;
        memmove(free_p - sizeof(slot_t), free_p, shift_size);
        memcpy(r->slot_p - sizeof(slot_t), &slot, sizeof(slot_t));
      }

      // update metadata
      node->h->data_off += entry->size;
      node->h->free_off -= sizeof(slot_t);
      node->h->free_size -= entry->size + sizeof(slot_t);
      if (node->h->is_leaf) { ++(dh_->num_keys); }
      ++(node->h->num_keys);
    }

    // [TODO] key might not be a string
    find_res_t *find_key(node_t *node, const void *key, uint32_t key_size)
    {
      find_res_t *r = new find_res_t;
      char *slot_p = (char *) node->b + node->h->free_off;
      slot_t *slots = (slot_t *) slot_p;

      if (node->h->num_keys == 0) {
        r->type = KEY_SMALLEST;
        return r;
      }

      // [TODO] regard the key as a string
      ALLOC_AND_COPY(entry_key, key, key_size);

      char checked[node->h->num_keys];
      memset(checked, 0, node->h->num_keys);

      // binary search
      int low_bound = 0;
      int up_bound = node->h->num_keys - 1;
      int middle = node->h->num_keys / 2;
      bool is_found = false;
      bool is_going_upper = false;
      int last_middle = -1;
      while (1) {
        // the key is not found if it's already checked
        if (checked[middle]) { break; }
        checked[middle] = 1;

        slot_t *slot = slots + middle;
        ALLOC_AND_COPY(stored_key, (char *) node->b + slot->off, slot->size);

        int res = strcmp(entry_key, stored_key);
        if (res == 0) {
          // found
          is_found = true;
          r->data_p = (char *) node->b + slot->off;
          break;
        } else if (res < 0) {
          // entry key is smaller (going to upper offset)
          low_bound = middle;
          last_middle = middle;
          div_t d = div(up_bound - middle, 2);
          middle = d.rem > 0 ? middle + d.quot + 1 : middle + d.quot;
          is_going_upper = true;
        } else {
          // entry key is bigger (going to lower offset)
          up_bound = middle;
          last_middle = middle;
          middle = low_bound + (middle - low_bound) / 2;
          is_going_upper = false;
        }
      }

      slot_t *slot = slots + middle;
      r->slot_p = (char *) slot;

      if (is_found) {
        r->type = KEY_FOUND;
      } else {
        if (is_going_upper) {
          if (middle == last_middle) {
            r->type = KEY_SMALLEST;
          } else {
            r->type = KEY_BIGGER;
          }
        } else {
          if (middle == last_middle) {
            r->type = KEY_BIGGEST;
          } else {
            slot = slots + (++middle);
            r->slot_p = (char *) slot;
            r->type = KEY_BIGGER;
          }
        }
      }
      return r;
    }

    bool append_page(void)
    {
      uint32_t num_nodes = dh_->num_nodes;
      uint16_t node_size = dh_->node_size;

      if (munmap(map_, node_size * num_nodes) < 0) {
        std::cerr << "munmap failed" << std::endl;
        return false;
      }
      ++num_nodes; // one page appendiing
      return alloc_page(num_nodes, node_size);
    }

    bool alloc_page(uint32_t num_nodes, uint16_t node_size)
    {
      if (ftruncate(fd_, node_size * num_nodes) < 0) {
        std::cout << "ftruncate failed" << std::endl;
        return false;
      }
      map_ = (char *) mmap(0, node_size * num_nodes, 
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);  
      if (map_ == MAP_FAILED) {
        std::cout << "map failed" << std::endl;
        return false;
      }
      dh_ = (db_header_t *) map_;
      dh_->num_nodes = num_nodes;
      return true;
    }
    
    bool split_node(node_t *node, node_t *new_node, up_entry_t **up_entry)
    {
      char *b = (char *) node->b;
      char *new_b = (char *) new_node->b;
      node_header_t *h = node->h;
      node_header_t *new_h = new_node->h;

      // current node slots
      slot_t *slots = (slot_t *) (b + h->free_off);

      // stay_num entries stay in the node, others move to the new node
      uint32_t num_stays = h->num_keys / 2;
      uint32_t num_moves = h->num_keys - num_stays;

      // [warn] slots size might 1
      // get a entry being set in the parent node  
      *up_entry = get_up_entry(node, slots + num_moves - 1, new_h->id);
      if (!h->is_leaf) {
        if (num_moves == 1) {
          std::cerr << "[error] shoud set enough page size for a node" << std::endl;
          return false;
        } else {
          --num_moves; // the entry is pushed up in non-leaf node
        }
      }

      uint16_t off = 0;
      // needs left most pointer in non-leaf node
      if (!node->h->is_leaf) {
        uint16_t leftmost_off = (slots + num_moves)->off + (slots + num_moves)->size;
        make_leftmost_ptr(new_node, (char *) node->b + leftmost_off);
        off += sizeof(node_id_t);
      }

      // copy the bigger entries to the new node
      char *slot_p = (char *) new_node->b + dh_->init_data_size;
      for (int i = num_moves - 1; i >= 0; --i) {
        // copy entry to the new node's data area
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + sizeof(uint32_t);
        memcpy(new_b + off, b + (slots+i)->off, entry_size);
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        slot_p -= sizeof(slot_t);
        memcpy(slot_p, &slot, sizeof(slot_t));
        off += entry_size;
      }
      set_node_header(new_h, off, num_moves);
      
      // copy staying entries into the buffers
      char dbuf[dh_->node_size], sbuf[dh_->node_size];
      char *dp = dbuf;
      char *sp = sbuf + dh_->node_size; // pointing the tail
      char tmp_node[dh_->node_size];
      off = 0;

      // needs left most pointer in non-leaf node
      if (!node->h->is_leaf) {
        // it's workaround
        memcpy(dp, (char *) node->b, sizeof(node_id_t));
        off += sizeof(node_id_t);
      }

      // copy entry to the data buffer
      for (int i = h->num_keys - 1; i >= h->num_keys - num_stays; --i) {
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + sizeof(uint32_t);
        memcpy(dp + off, b + (slots+i)->off, entry_size);
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        sp -= sizeof(slot_t);
        memcpy(sp, &slot, sizeof(slot_t));
        off += entry_size;
      }

      // copy the buffers to the node
      slot_p = (char *) node->b + dh_->init_data_size;
      uint16_t slots_size = sizeof(slot_t) * num_stays;
      memcpy(b, dbuf, off); 
      memcpy(slot_p - slots_size, sp, slots_size);

      set_node_header(h, off, num_stays);

      return true;
    }

    // make a left most pointer in non-leaf node
    void make_leftmost_ptr(node_t *node, char *ptr)
    {
       memcpy((char *) node->b, ptr, sizeof(node_id_t));
       node->h->data_off += sizeof(node_id_t);
       node->h->free_size -= sizeof(node_id_t);
    }

    void set_node_header(node_header_t *h, uint16_t off, uint16_t num_keys)
    {
      h->data_off = off;
      h->free_off = dh_->init_data_size - num_keys * sizeof(slot_t);
      h->free_size = h->free_off - h->data_off;
      h->num_keys = num_keys;
    }

    /*
     * [TODO] decide if to use
     * copy entries and slots from a specified node to a buffer specified by dp and sp.
     * offsets in slots are modified along with entry move.
     */
    void copy_entry(char *dp, char *sp, node_t *node, slot_t *slots, int slot_from, int slot_to)
    {
      uint16_t off = 0;
      char *slot_p = (char *) node->b + dh_->init_data_size;
      for (int i = slot_from; i >= slot_to; --i) {
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + sizeof(uint32_t);
        memcpy(dp, (char *) node->b + (slots+i)->off, entry_size);
        dp += entry_size;
        // new slot for the entry above
        slot_t slot = { off, (slots+i)->size };
        slot_p -= sizeof(slot_t);
        memcpy(slot_p, &slot, sizeof(slot_t));
        off += entry_size;
      }
    }

    up_entry_t *get_up_entry(node_t *node, slot_t *slot, node_id_t up_node_id)
    {
      char *b = (char *) node->b;
      up_entry_t *up_entry = new up_entry_t;
      up_entry->key = new char[slot->size];
      memcpy((char *) up_entry->key, b + slot->off, slot->size);
      up_entry->key_size = slot->size;
      up_entry->val = new char[sizeof(node_id_t)];
      memcpy((char *) up_entry->val, &up_node_id, sizeof(node_id_t));
      up_entry->val_size = sizeof(node_id_t);
      up_entry->size = up_entry->key_size + up_entry->val_size;

      return up_entry;
    } 

    void clean_up_entry(up_entry_t **up_entry)
    {
        delete [] (char *) (*up_entry)->key;
        delete [] (char *) (*up_entry)->val;
        delete *up_entry;
        *up_entry = NULL;
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
