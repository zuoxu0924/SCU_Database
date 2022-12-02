/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() : pSize(0) {
  head = new node();
  tail = new node();
  head->next = tail;
  tail->prev = head;
}

/*
 * In this task, I am peimitted that I could assume I will not run out of memory
 * So there is no 'actual' implement with deconstructor
 */
template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lck(mtx);
  node* temp;
  if(pages.find(value) == pages.end()) {
    //之前没有出现过的page，要新加入则new一个节点以记录其信息
    temp = new node(value);
    pSize++;
  } else {
    //之前用过的page再被提及到，则找到这个节点
    temp = pages[value];
    node* pre = temp->prev;
    node* nxt = temp->next;
    pre->next = nxt;
    nxt->prev = pre;
  }
  //把最新的节点放在队首
  node* first = head->next;
  temp->next = first;
  first->prev = temp;
  temp->prev = head;
  head->next = temp;
  pages[value] = temp;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lck(mtx);
  // If LRU is empty, return false
  if(pSize == 0) return false;
  // If LRU is non-empty
  //目前在队尾的节点是least-recently used
  node* temp = tail->prev;
  value = temp->data;
  tail->prev = temp->prev;
  temp->prev->next = tail;
  pages.erase(temp->data);
  pSize--;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lck(mtx);
  if(pages.find(value) != pages.end()) {
    node* temp = pages[value];
    temp->prev->next = temp->next;
    temp->next->prev = temp->prev;
    pSize--;
  }
  return pages.erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  std::lock_guard<std::mutex> lck(mtx);
  return pSize; 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
