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

#ifndef LUX_DBM_BTREE_H
#define LUX_DBM_BTREE_H

#include "dbm.h"
#include "data.h"

#define ALLOC_AND_COPY(s1, s2, size) \
  char s1[size+1]; \
  memcpy(s1, s2, size); \
  s1[size] = '\0';

namespace Lux {
namespace DBM {

  const char *MAGIC = "LUXBT001";
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
    uint32_t num_nodes;
    uint16_t node_size;
    uint16_t init_data_size;
    uint32_t root_id;
    uint32_t num_leaves;
    uint32_t num_nonleaves;
    uint8_t index_type;
    uint8_t data_size; // for fixed length value in cluster index
  } btree_header_t;

  typedef uint32_t node_id_t;
  typedef struct {
    bool is_root;
    bool is_leaf;
    uint32_t id;
    uint16_t num_keys;
    uint16_t data_off;
    uint16_t free_off;
    uint16_t free_size;
    uint32_t prev_id; // used only in leaf
    uint32_t next_id; // used only in leaf
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
    uint32_t size; // entry size stored in pages
    insert_mode_t mode;
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

  // comparison functions
  int str_cmp_func(data_t &d1, data_t &d2)
  {
    return strcmp((char *) d1.data, (char *) d2.data);
  }
  int int32_cmp_func(data_t &d1, data_t &d2)
  {
    int32_t i1, i2;
    memcpy(&i1, d1.data, sizeof(int32_t));
    memcpy(&i2, d2.data, sizeof(int32_t));
    return (i1 - i2); 
  };
  int uint32_cmp_func(data_t &d1, data_t &d2)
  {
    uint32_t i1, i2;
    memcpy(&i1, d1.data, sizeof(int32_t));
    memcpy(&i2, d2.data, sizeof(int32_t));
    if (i1 < i2) return -1; 
    else if (i1 == i2) return 0;
    else return 1;
  };
  typedef int (*CMP)(data_t &d1, data_t &d2);

  /*
   * Class Btree
   */
  class Btree {
  public:
    Btree(db_index_t index_type = NONCLUSTER,
          uint8_t data_size = sizeof(uint32_t))
    : cmp_(str_cmp_func),
      index_type_(index_type),
      dt_(index_type == NONCLUSTER ? new LinkedData() : NULL),
      data_size_(index_type == NONCLUSTER ? sizeof(data_ptr_t) : data_size)
    {}

    ~Btree()
    {}

    bool open(std::string db_name, db_flags_t oflags)
    {
      std::string idx_db_name = db_name + ".bidx";
      fd_ = _open(idx_db_name.c_str(), oflags, 00644);
      if (fd_ < 0) {
        return false;
      }

      struct stat stat_buf;
      if (fstat(fd_, &stat_buf) == -1 || !S_ISREG(stat_buf.st_mode)) {
        return false;
      }

      btree_header_t dh;
      memset(&dh, 0, sizeof(btree_header_t));
      if (stat_buf.st_size == 0 && oflags & DB_CREAT) {
        // initialize the header for the newly created file
        //strncpy(h.magic, MAGIC, strlen(MAGIC));
        dh.num_keys = 0;
        // one for db_header, one for root node and one for leaf node
        dh.num_nodes = 3;
        dh.node_size = getpagesize();
        //dh.node_size = 128;
        dh.init_data_size = dh.node_size - sizeof(node_header_t);
        dh.root_id = 1;
        dh.num_leaves = 0;
        dh.num_nonleaves = 0; // root node
        dh.index_type = index_type_;
        dh.data_size = data_size_;

        if (_write(fd_, &dh, sizeof(btree_header_t)) < 0) {
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
        if (_read(fd_, &dh, sizeof(btree_header_t)) < 0) {
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
      dh_ = (btree_header_t *) map_;
      if (dh_->index_type == NONCLUSTER) {
        std::string data_db_name = db_name + ".data";
        dt_->open(data_db_name.c_str(), oflags);
      }
    }

    bool close()
    {
      msync(map_, dh_->node_size * dh_->num_nodes, MS_SYNC);
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

    bool put(const void *key, uint32_t key_size,
             const void *val, uint32_t val_size, insert_mode_t flags = OVERWRITE)
    {
      entry_t entry = {(char *) key, key_size,
                       (char *) val, val_size,
                       key_size + dh_->data_size,
                       flags};
      up_entry_t *up_entry = NULL;
    
      insert(dh_->root_id, &entry, &up_entry);
    }

    bool del(const void *key, uint32_t key_size)
    {
      entry_t entry = {key, key_size, NULL, 0, 0};

      _del(dh_->root_id, &entry);
    }

    bool _del(node_id_t id, entry_t *entry)
    {
      node_t *node = _alloc_node(id);
      if (node->h->is_leaf) {
        find_res_t *r = find_key(node, entry->key, entry->key_size);
        if (r->type == KEY_FOUND) {
          // remove a slot
          char *p = (char *) node->b + node->h->free_off;
          if (p != r->slot_p) {
            memmove(p + sizeof(slot_t), p, r->slot_p - p);
          }
          node->h->free_off += sizeof(slot_t);
          node->h->free_size += sizeof(slot_t);
          --(node->h->num_keys);
          --(dh_->num_keys);
        }
        delete r;
      } else {
        node_id_t next_id = _find_next(node, entry);
        _del(next_id, entry);
      }
      delete node;
    }

    bool set_cmp_func(CMP cmp)
    {
      cmp_ = cmp;
    }

    void show_node(void)
    {
      show_db_header();
      for (int i = 1; i < dh_->num_nodes; ++i) {
        show_node(i);
      }
    }

    void show_root(void)
    {
      show_node(dh_->root_id);
    }

    void show_db_header()
    {
      std::cout << "========= SHOW ROOT ==========" << std::endl;
      std::cout << "num_keys: " << dh_->num_keys << std::endl;
      std::cout << "num_nodes: " << dh_->num_nodes << std::endl;
      std::cout << "node_size: " << dh_->node_size << std::endl;
      std::cout << "init_data_size: " << dh_->init_data_size << std::endl;
      std::cout << "root_id: " << dh_->root_id << std::endl;
      std::cout << "num_leaves: " << dh_->num_leaves << std::endl;
      std::cout << "num_nonleaves: " << dh_->num_nonleaves << std::endl;
      std::cout << "index_type: " << (int) dh_->index_type << std::endl;
      std::cout << "data_size: " << (int) dh_->data_size << std::endl;
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
    btree_header_t *dh_;
    db_index_t index_type_;
    uint8_t data_size_;
    CMP cmp_;
    Data *dt_;

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
      node_hdr_p->prev_id = 0; // 0 means no link
      node_hdr_p->next_id = 0; // 0 means no link
      node_body_t *node_body_p = (node_body_t *) (node_p + sizeof(node_header_t));

      node_t *node = new node_t;
      node->h = node_hdr_p;
      node->b = node_body_p;

      if (is_leaf) {
        ++(dh_->num_leaves);
      } else {
        ++(dh_->num_nonleaves);
      }

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
          *val_data = get_data(r);
        }
        delete r;
      } else {
        node_id_t next_id = _find_next(node, &entry);
        find(next_id, key_data, val_data);
      }
      delete node;
    }

    data_t *get_data(find_res_t *r)
    {
      slot_t *slot = (slot_t *) r->slot_p;
      data_t *val_data;
      if (dh_->index_type == CLUSTER) {
        val_data = new data_t;
        val_data->data = (char *) new char[dh_->data_size];
        val_data->size = dh_->data_size;
        memcpy((void *) val_data->data, 
               (const char *) r->data_p + slot->size, dh_->data_size);
      } else {
        data_ptr_t data_ptr;
        memcpy(&data_ptr, (const char *) r->data_p + slot->size, sizeof(data_ptr_t));
        val_data = dt_->get(&data_ptr);
      }
      return val_data;
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
          if (update_if_exists(node, entry)) {
            delete node;
            return;
          }

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

        delete node;
        if (*up_entry == NULL) { return; }

        // must reallocate after remapped
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
          data_t e_data = {e->key, e->key_size};
          data_t up_e_data = {(*up_entry)->key, (*up_entry)->key_size};

          if (cmp_(e_data, up_e_data) < 0) {
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
            // change root 
            dh_->root_id = new_root->h->id;
            node->h->is_root = false;
            delete new_root;
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
      delete r;
      return id;
    }

    bool update_if_exists(node_t *node, entry_t *entry)
    {
      bool is_found = false;
      find_res_t *r = find_key(node, entry->key, entry->key_size);
      if (r->type == KEY_FOUND) {
        // update the value
        // [TODO] append
        memcpy((char *) r->data_p + entry->key_size, entry->val, entry->val_size);
        is_found = true;
      }
      delete r;
      return is_found;
    }

    void put_entry_in_leaf(node_t *node, entry_t *entry)
    {
      entry_t _entry = {entry->key, entry->key_size,
                        NULL, dh_->data_size, entry->size, entry->mode};

      find_res_t *r = find_key(node, entry->key, entry->key_size);

      if (r->type == KEY_FOUND) {
        if (entry->mode == NOOVERWRITE) {
          delete r;
          return;
        }
        if (dh_->index_type == CLUSTER) {
          // append is not supported in b+-tree cluster index. only updating
          memcpy((char *) r->data_p + _entry.key_size, _entry.val, _entry.val_size);
        } else {
          data_ptr_t data_ptr;
          memcpy(&data_ptr, (char *) r->data_p + entry->key_size, sizeof(data_ptr_t));
          data_t data = {entry->val, entry->val_size};
          data_ptr_t *res_data_ptr;

          // append or update the data, get the ptr to the data and update the index
          if (entry->mode == APPEND) {
            res_data_ptr = dt_->append(&data_ptr, &data);
          } else { // OVERWRITE
            res_data_ptr = dt_->update(&data_ptr, &data);
          }
          char val[dh_->data_size];
          memcpy(&val, res_data_ptr, sizeof(data_ptr_t));
          _entry.val = val;
          put_entry(node, &_entry, r);

          dt_->clean_data_ptr(res_data_ptr);
        }

      } else {
        if (dh_->index_type == CLUSTER) {
          put_entry(node, entry, r);
        } else {
          data_t data = {entry->val, entry->val_size};
          data_ptr_t *res_data_ptr;

          // put the data, get the ptr to the data and update the index
          res_data_ptr = dt_->put(&data);
          char val[dh_->data_size];
          memcpy(&val, res_data_ptr, sizeof(data_ptr_t));
          _entry.val = val;
          put_entry(node, &_entry, r);

          dt_->clean_data_ptr(res_data_ptr);
        }
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

    // [TODO] API should change : take data_t instead of key and key_size
    find_res_t *find_key(node_t *node, const void *key, uint32_t key_size)
    {
      find_res_t *r = new find_res_t;
      char *slot_p = (char *) node->b + node->h->free_off;
      slot_t *slots = (slot_t *) slot_p;

      if (node->h->num_keys == 0) {
        r->type = KEY_SMALLEST;
        return r;
      }

      // [TODO] API should change : take data_t instead of key and key_size
      data_t key_data = {key, key_size};

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
        data_t stored_data = {stored_key, slot->size};

        int res = cmp_(key_data, stored_data);
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

    // [TODO] append_node is better naming ?
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

    // [TODO] alloc_node is better naming ?
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
      dh_ = (btree_header_t *) map_;
      dh_->num_nodes = num_nodes;
      return true;
    }
    
    bool split_node(node_t *node, node_t *new_node, up_entry_t **up_entry)
    {
      // current node slots
      slot_t *slots = (slot_t *) ((char *) node->b + node->h->free_off);

      // stay_num entries stay in the node, others move to the new node
      uint16_t num_stays = node->h->num_keys / 2;
      uint16_t num_moves = node->h->num_keys - num_stays;

      // [SPEC] a node must contain at least 4 entries
      if (num_stays <= 2 || num_moves <= 2) {
        std::cerr << "[error] the number of entries in a node is too small." << std::endl;
        return false;
      }

      // get a entry being set in the parent node  
      *up_entry = get_up_entry(node, slots, num_moves, new_node->h->id);
      if (!node->h->is_leaf) {
        if (num_moves == 1) {
          // [TODO] throwing error for now
          throw std::runtime_error("something bad happened. never comes here usually.");
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
      copy_entries((char *) new_node->b, slot_p, node, slots, off, num_moves-1, 0);
      set_node_header(new_node->h, off, num_moves);
      // make a link
      new_node->h->prev_id = node->h->id;
      
      // copy staying entries into the buffers
      char tmp_node[dh_->node_size];
      node_t n;
      node_t *np = &n;
      np->b = (node_body_t *) (tmp_node + sizeof(node_header_t));
      off = 0;

      // needs left most pointer in non-leaf node
      if (!node->h->is_leaf) {
        memcpy((char *) np->b, (char *) node->b, sizeof(node_id_t));
        off += sizeof(node_id_t);
      }

      // copy entry to the data buffer
      slot_p = (char *) np->b + dh_->init_data_size;
      copy_entries((char *) np->b, slot_p, node, slots, off,
                   node->h->num_keys-1, node->h->num_keys-num_stays);

      // copy the buffers to the node and update the header
      memcpy((char *) node->b, (char *) np->b, dh_->init_data_size);
      set_node_header(node->h, off, num_stays);
      // make a link
      node->h->next_id = new_node->h->id;

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
     * copy entries and slots from a specified node to a buffer specified by dp and sp.
     * offsets in slots are updated in each entry move.
     */
    void copy_entries(char *dp, char *sp, node_t *node, slot_t *slots,
                      uint16_t &data_off, int slot_from, int slot_to)
    {
      //sp = (char *) node->b + dh_->init_data_size;
      for (int i = slot_from; i >= slot_to; --i) {
        // [TODO] value size is sizeof(uint32_t) for now
        uint32_t entry_size = (slots+i)->size + dh_->data_size;
        memcpy(dp + data_off, (char *) node->b + (slots+i)->off, entry_size);
        //dp += entry_size;
        // new slot for the entry above
        slot_t slot = { data_off, (slots+i)->size };
        sp -= sizeof(slot_t);
        memcpy(sp, &slot, sizeof(slot_t));
        data_off += entry_size;
      }
    }

    // get prefix between a big key and a small key
    char *get_prefix_key(char *big, char *small)
    {
      size_t len = strlen(big) > strlen(small) ? strlen(small) : strlen(big);
      char *prefix = new char[len+2];
      memset(prefix, 0, len+2);

      int prefix_off = 0;
      for (int i = 0; i < len; ++i, ++prefix_off) {
        if (big[i] != small[i]) {
          break;
        }
      }
      memcpy(prefix, big, prefix_off+1);
      return prefix;
    }

    up_entry_t *get_up_entry(node_t *node, slot_t *slots, 
                              uint16_t boundary_off, node_id_t up_node_id)
    {
      up_entry_t *up_entry = new up_entry_t;

      if (cmp_ == str_cmp_func && node->h->is_leaf) {
        slot_t *slot_r = slots + boundary_off; // right slot (smaller)
        slot_t *slot_l = slots + boundary_off - 1; // left slot (bigger)
        ALLOC_AND_COPY(key_small, (char *) node->b + slot_r->off, slot_r->size);
        ALLOC_AND_COPY(key_big, (char *) node->b + slot_l->off, slot_l->size);
        // get prefix key for prefix key compression
        up_entry->key = get_prefix_key(key_big, key_small);
        up_entry->key_size = strlen((char *) up_entry->key);
      } else {
        slot_t *slot = slots + boundary_off - 1;
        up_entry->key = new char[slot->size];
        memcpy((char *) up_entry->key, (char *) node->b + slot->off, slot->size);
        up_entry->key_size = slot->size;
      }
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

  };

}
}

#endif
