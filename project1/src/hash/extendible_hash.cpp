#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : 
  bucket_size(size), global_depth(0), bucket_num(1) {
    dict.push_back(std::make_shared<Bucket>(0, 0));
}

template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>{}(key);

}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  //互斥访问
  std::lock_guard<std::mutex> lck(mtx);
  return global_depth;

}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  // std::lock_guard<std::mutex> lck(mtx);
  if(dict[bucket_id]) {
    std::lock_guard<std::mutex> lck2(dict[bucket_id]->mtx);
    if(dict[bucket_id]->records.size() != 0) {
      return dict[bucket_id]->local_depth;
    }
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lck(mtx);
  return bucket_num;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  
  std::lock_guard<std::mutex> lck(mtx);
  size_t pos = HashKey(key) & ((1 << global_depth) - 1);
  bool isFound = false;
  // std::lock_guard<std::mutex> lck2(dict[pos]->mtx);
  //iterator没有迭代到最后就是找到了key对应的value
  if(dict[pos]) {
    if(dict[pos]->records.find(key) != dict[pos]->records.end()) {
        value = dict[pos]->records[key];
        isFound = true;
    }
  }

  return isFound;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {

  std::lock_guard<std::mutex> lck(mtx);
  size_t pos = HashKey(key) & ((1 << global_depth) - 1);

  // std::lock_guard<std::mutex> lck2(dict[pos]->mtx);
  std::shared_ptr<Bucket> temp = dict[pos];
  if(temp->records.find(key) == temp->records.end()) {
    return false;
  }
  temp->records.erase(key);
  return true;

}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  /*
  * local depth <= global depth
  * if not overflow just insert
  * if overflow
  *   if local depth < global depth 
  *     just split bucket
  *   else
  *     split bucket & expand dictionary
  */
  std::lock_guard<std::mutex> lck(mtx);
  size_t pos = HashKey(key) & ((1 << global_depth) - 1);
  auto bucket = dict[pos];
  // std::lock_guard<std::mutex> lck2(bucket->mtx);
  if(bucket->records.find(key) != bucket->records.end()) {
        bucket->records[key] = value;
        return;
  }

  bucket->records.insert({key, value});
  
  // std::lock_guard<std::mutex> lck3(mtx);
  if(bucket->records.size() > bucket_size) {
    auto oId = bucket->id_t;
    auto lDepth = bucket->local_depth;
    std::shared_ptr<Bucket> newBuc = split(bucket);

    if(bucket->local_depth > global_depth) {
      auto size = dict.size();
      // auto mask = (1 << (bucket->local_depth - global_depth));
      global_depth = bucket->local_depth;
      dict.resize((int)pow(2, global_depth));

      dict[bucket->id_t] = bucket;
      dict[newBuc->id_t] = newBuc;

      for(size_t i = 0; i < size; i++) {
        if(dict[i] && i >= dict[i]->id_t) {
          auto offset = 1 << dict[i]->local_depth;
          for(size_t j = i + offset; j < dict.size(); j += offset) dict[j] = dict[i];
        }
      }
    } else {
      for(size_t i = oId; i < dict.size(); i += (1 << lDepth)) dict[i].reset();

      dict[bucket->id_t] = bucket;
      dict[newBuc->id_t] = newBuc;

      auto offset = 1 << bucket->local_depth;
      for(size_t i = bucket->id_t + offset; i < dict.size(); i += offset) dict[i] = bucket;
      for(size_t i = newBuc->id_t + offset; i < dict.size(); i += offset) dict[i] = newBuc;
    }

  }

}

// template<typename K, typename V>
// size_t ExtendibleHash<K, V>::getId(const K &key) const {
//   std::lock_guard<std::mutex> lck(mtx);
//   return HashKey(key) & ((1 << global_depth) - 1);
// }


template<typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> 
ExtendibleHash<K, V>::split(std::shared_ptr<Bucket> &bucket){
  std::lock_guard<std::mutex> lck(bucket->mtx);
  auto newBucket = std::make_shared<Bucket>(0, bucket->local_depth);

  while(newBucket->records.empty()) {
    bucket->local_depth++;
    newBucket->local_depth++;

    // int mask = (1 << (bucket->local_depth - 1));
    for(auto it = bucket->records.begin(); it != bucket->records.end(); ) {
      if(HashKey(it->first) & (1 << (bucket->local_depth - 1))) {
        newBucket->records.insert({it->first, it->second});
        newBucket->id_t = HashKey(it->first) & ((1 << bucket->local_depth) - 1);
        it = bucket->records.erase(it);
      } else it++;
    }

    if(bucket->records.empty()) {
      size_t i = 0;
      for(auto it = newBucket->records.begin(); i <= newBucket->records.size() / 2; i++) {
        bucket->records.insert({it->first, it->second});
        it = newBucket->records.erase(it);
      }
      bucket->id_t = newBucket->id_t;
    }
  }

  bucket_num++;

  return newBucket;
}


template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb

