//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto new_page_id = AllocatePage();

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
  }

  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;
  page_table_[new_page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto pair = page_table_.find(page_id);
  if (pair != page_table_.end()) {
    return &pages_[pair->second];
  }

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
  }

  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }

  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto pair = page_table_.find(page_id);
  if (pair == page_table_.end()) {
    return false;
  }
  auto page = &pages_[pair->second];
  if (page->pin_count_ == 0) return false;
  page->pin_count_ -= 1;
  if (page->pin_count_ <= 0) {
    replacer_->SetEvictable(pair->second, true);
  }

  page_table_.erase(page_id);
  page->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) return false;

  auto pair = page_table_.find(page_id);
  if (pair == page_table_.end()) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[pair->second].data_);
  pages_[pair->second].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; ++i) {
    auto page = &pages_[i];
    FlushPage(page->page_id_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto pair = page_table_.find(page_id);
  if (pair == page_table_.end()) {
    return true;
  }

  auto page = &pages_[pair->second];
  if (page->pin_count_ > 0) {return false;}

  auto frame_id = pair->second;
  replacer_->Remove(frame_id);
  page->ResetMemory();
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  auto page = FetchPage(page_id);
  
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  
  return {this, page};
}

}  // namespace bustub
