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
    dict.push_back(std::make_shared<Bucket>(0));
}

//template<typename K, typename V>
//ExtendibleHash<K, V>::ExtendibleHash() {
//  ExtendibleHash(64);
//}

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
  return this->global_depth;

}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  std::lock_guard<std::mutex> lck(mtx);
  if(dict[bucket_id]) {
    //std::lock_guard<std::mutex> lck2(dict[bucket_id]->mtx);
    if(dict[bucket_id]->records.size() == 0) {
      return -1;
    }
    return dict[bucket_id]->local_depth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lck(mtx);
  return this->bucket_num;
}

/*
 * get bucket index in dictionary
 */
template<typename K, typename V>
size_t ExtendibleHash<K, V>::getId(const K &key) {
    //lock_guard<mutex> lck(mtx);
    return HashKey(key) & ((1 << global_depth) - 1);
}
  
/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> lck(mtx);
    bool isFound = false;
    size_t pos = getId(key);
    //lock_guard<mutex> lck(dict[pos]->mtx);
    if(dict[pos]) {
        if (dict[pos]->records.find(key) != dict[pos]->records.end()) {
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
  bool isRemoved = false;
  size_t pos = getId(key);
  //std::lock_guard<std::mutex> lck2(dict[pos]->mtx);
  std::shared_ptr<Bucket> temp = dict[pos];
  if(temp->records.find(key) == temp->records.end()) return isRemoved;
  temp->records.erase(key);
  isRemoved = true;
  return isRemoved;

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
  size_t pos = getId(key);
  std::shared_ptr<Bucket> temp = dict[pos];
  while (temp->records.size() == this->bucket_size) {
    if(temp->local_depth == this->global_depth) {
      size_t size = dict.size();
      for (size_t i = 0; i < size; i++) {
        dict.push_back(dict[i]);
      }
      global_depth++;
      bucket_num++;
    }
    int mask = 1 << temp->local_depth;
        
    std::shared_ptr<Bucket> b1 = std::make_shared<Bucket>(temp->local_depth + 1);
    std::shared_ptr<Bucket> b2 = std::make_shared<Bucket>(temp->local_depth + 1);

    for(auto record : temp->records) {
      size_t new_key = HashKey(record.first);

      if(new_key & mask)  b1->records.insert(record);
      else    b2->records.insert(record);
    }
    
    size_t size = dict.size();
    for(size_t i = 0; i < size; i++) {
      if(dict[i] == temp) {
        if(i & mask)    dict[i] = b1;
        else    dict[i] = b2;
      }
    }

    pos = getId(key);
    temp = dict[pos];
  }

  dict[pos]->records[key] = value;

}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb

