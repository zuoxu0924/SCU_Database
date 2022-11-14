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
  return global_depth;

}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  std::lock_guard<std::mutex> lck(mtx);
  if(dict[bucket_id]) {
    std::lock_guard<std::mutex> lck2(dict[bucket_id]->mtx);
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
  return bucket_num;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  
  //std::lock_guard<std::mutex> lck(mtx);
  //size_t pos = HashKey(key) & ((1 << global_depth) - 1);
  bool isFound = false;
  size_t pos = getId(key);
  std::lock_guard<std::mutex> lck2(dict[pos]->mtx);
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

  //std::lock_guard<std::mutex> lck(mtx);
  //size_t pos = HashKey(key) & ((1 << global_depth) - 1);
  bool isRemoved = false;
  size_t pos = getId(key);
  std::lock_guard<std::mutex> lck2(dict[pos]->mtx);
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
  size_t pos = getId(key);
  std::shared_ptr<Bucket> temp = dict[pos];
  while (true) {
    std::lock_guard<std::mutex> lck(temp->mtx);
    if (temp->records.find(key) != temp->records.end() || temp->records.size() < bucket_size) {
      temp->records[key] = value;
      break;
    }
    int mask = 1 << (temp->local_depth);
    temp->local_depth++;
    if (temp->local_depth > global_depth) {
      size_t size = dict.size();
      for (size_t i = 0; i < size; i++) {
        dict.push_back(dict[i]);
      }
      global_depth++;
    }
    bucket_num++;
    
    size_t newPos = HashKey(temp->records.begin()->first) & ((1 << temp->local_depth) - 1);
    std::shared_ptr<Bucket> newBucket = std::make_shared<Bucket>(newPos, temp->local_depth);

    typename std::map<K, V>::iterator it;
    for (it = temp->records.begin(); it != temp->records.end(); ) {
      if (HashKey(it->first) & mask) {
        newBucket->records[it->first] = it->second;
        it = temp->records.erase(it);
      } else it++;
    }
    for (size_t i = 0; i < dict.size(); i++) {
      if (dict[i] == temp && (i & mask)) dict[i] = newBucket;
    }
    pos = getId(key);
    temp = dict[pos];
  }

}

template<typename K, typename V>
size_t ExtendibleHash<K, V>::getId(const K &key) {
  std::lock_guard<std::mutex> lck(mtx);
  return HashKey(key) & ((1 << global_depth) - 1);
}


/*template<typename K, typename V>
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
*/

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb

