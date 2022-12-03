/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
	this->SetPageType(IndexPageType::INTERNAL_PAGE);	// set page type
	this->SetSize(1);	// set current size
	this->SetPageId(page_id);	// set page id
	this->SetParentPageId(parent_id);	// set parent id
	// set max page size
	int _max_page_size = PAGE_SIZE - sizeof(BPlusTreeInternalPage) / sizeof(MappingType);
	this->SetMaxSize(_max_page_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  if(index < 0 || index > GetSize()) {	// index out of range 返回空
	return {};
  }
//   返回对应键
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
	if(index >= 0 && index < GetSize()) {	// valid index then set "key"
		array[index].first = key;
	}
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
	for(int i = 0; i < GetSize(); i++) {
		if(array[i].second == value) {
			return i;
		}
	}
	// 没找到则返回-1
	return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
	if(index < 0 || index > GetSize()) {	// index out of range
		return {};
	}
	// else return value at "index"
	return array[index].second; 
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
	// 判断给定索引是否越界
	assert(index >= 0 && index < GetSize());
	array[index].second = value;
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
	int left = 1;	//start with the second key
	int right = GetSize();
	
	while(left < right) {	//binary search
		int _mid = (right - left) / 2 + left;
		if(comparator(array[_mid].first, key) == -1) {
			left = _mid + 1;
		} else {
			right = _mid;
		}
	}
	return array[left - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
		array[0].second = old_value;
		array[1].first = new_key;
		array[1].first = new_value;
		IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
		for(int i = GetSize(); i > 0; i--) {
			if(array[i - 1].second == old_value) {
				array[i] = {new_key, new_value};
				break;
			}
			array[i].first = array[i - 1].first;
			array[i].second = array[i - 1].second;
		}
		IncreaseSize(1);
		// return:  new size after insertion
		return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
		auto _half = (GetSize() + 1) / 2;
		recipient->CopyHalfFrom(array + GetSize() - _half, _half, buffer_pool_manager);
		//更新子节点的父节点
		for(int i = GetSize() - _half; i < GetSize(); i++) {
			Page* page = buffer_pool_manager->FetchPage(ValueAt(i));
			if(page == nullptr) {
				throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
			}

			BPlusTreeInternalPage* _child = reinterpret_cast<BPlusTreePage *>(page->GetData());
			child->SetParentPageId(recipient->GetPageId());
			buffer_pool_manager->UnpinPage(page->GetPageId(), true);
		}
		IncreaseSize(-1 * _half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
		int _pos = GetSize();
		for(int i = 0; i < size; i++) {
			array[_pos + i] = *items++;
		} 
		IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
	for(int i = index; i < GetSize() - 1; i++) {
		array[i] = array[i + 1];
	}
	IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
	IncreaseSize(-1);
	return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
		// Remove all
		Page* _parent = buffer_pool_manager->FetchPage(this->GetParentPageId());
		if(_parent == nullptr) {
			throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
		}

		BPlusTreeInternalPage* internal_page = reinterpret_cast<BPlusTreeInternalPage *>(_parent->GetData());
		internal_page->SetValueAt(index_in_parent, recipient->GetPageId());
		buffer_pool_manager->UnpinPage(_parent->GetPageId(), true);
		recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
		// update
		for(int i = 0; i < GetSize(); i++) {
			auto *page = buffer_pool_manager->FetchPage(ValueAt(i));
			if(page == nullptr) {
				throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
			}
			auto *_child = reinterpret_cast<BPlusTreePage *>(page->GetData());
			_child->SetParentPageId(recipient->GetPageId());
			buffer_pool_manager->UnpinPage(_child->GetPageId(), true);
		}
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
		int _pos = GetSize();
		for(int i = 0; i < size; i++) {
			array[_pos + i] = *items++;
		}
		IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
		page_id_t page_id = ValueAt(0);
		MappingType pair{KeyAt(1), ValueAt(0)};
		SetValueAt(0, ValueAt(1));
		Remove(1);
		recipient->CopyLastFrom(pair, buffer_pool_manager);

		auto *page = buffer_pool_manager->FetchPage(page_id);
		if(page == nullptr) {
			throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
		}
		auto *_child = reinterpret_cast<BPlusTreePage *>(page->GetData());
		_child->SetParentPageId(recipient->GetPageId());
		buffer_pool_manager->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
		auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
		if(page == nullptr) {
			throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
		}
		auto *_parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
		auto index = _parent->ValueAt(GetPageId());
		auto _key = _parent->KeyAt(index + 1);
		array[GetSize()] = {_key, pair.second};
		IncreaseSize(1);
		_parent->SetKeyAt(index + 1, pair.first);

		buffer_pool_manager->UnpinPage(_parent->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
		IncreaseSize(-1);
		MappingType _pair = array[GetSize()];
		page_id_t _child_pid = pair.second;
		recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);

		auto *page = buffer_pool_manager->FetchPage(_child_pid);
		if(page == nullptr) {
			throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
		}

		auto *_child = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
		_child->SetParentPageId(recipient->GetPageId());

		buffer_pool_manager->UnpinPage(_child->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
		auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
		if(page == nullptr) {
			throw Exception(EXCEPTION_TYPE_INVALID, "there are no unpinned pages...");
		}
		
		auto *_parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
		auto _key = _parent->KeyAt(parent_index);
		_parent->SetKeyAt(parent_index, pair.first);
		InsertNodeAfter(array[0].second, _key, array[0].second);

		buffer_pool_manager->UnpinPage(_parent->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
