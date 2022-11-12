/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <mutex>
#include <memory>
#include <cmath>

#include "hash/hash_table.h"

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {

public:
  // constructor
  ExtendibleHash(size_t size=64);
  ExtendibleHash();
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  //local depth <= global depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  // size_t getId(const K &key) const;

private:
  struct Bucket{
        Bucket(size_t id, int localdepth) : id_t(id), local_depth(localdepth) {}
        size_t id_t = 0;
        int local_depth = 0;
        std::map<K, V> records;
        std::mutex mtx;
  };
  size_t bucket_size; //size of bucket
  int global_depth; 
  int bucket_num; //the number of buckets
  std::vector<std::shared_ptr<Bucket>> dict;  //dictionary to store the address of bucket
  mutable std::mutex mtx; //mutex lock thread security
  std::shared_ptr<Bucket> split(std::shared_ptr<Bucket> &bucket);  //split the bucket

};
} // namespace scudb
