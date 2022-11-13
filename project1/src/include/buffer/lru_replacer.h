/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
#include <unordered_map>

namespace scudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  // add your member variables here
  struct node
  {
    node() {}
    node(T val) : data(val) {}
    T data;
    node *prev = nullptr;
    node *next = nullptr;
  };
  mutable std::mutex mtx; //insure thread safety
  node* head; //head node
  node* tail; //tail node
  size_t pSize; //the number of pages
  std::unordered_map<T, node*> pages;
};

} // namespace scudb
