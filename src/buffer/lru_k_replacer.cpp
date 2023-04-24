//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  frame_id_t to_evict = -1;
  size_t current_distance = 0;
  size_t count_no_k_entry = 0;
  auto current_timestamp = CurrentTimestamp();
  for (auto const &a : node_store_) {
    auto node = a.second;
    if (!node.is_evictable_) {
      continue;
    }

    size_t distance = node.CalculateKDistance(current_timestamp);
    printf("frame id = %d - size= %ld\n", a.first, node.history_.size());
    if (distance == UINT64_MAX) {
      count_no_k_entry += 1;
    }
    if (distance > current_distance) {
      current_distance = distance;
      to_evict = a.first;
    }
  }

  if (count_no_k_entry > 1) {
    for ( auto &a : node_store_) {
      if (!a.second.is_evictable_) {
        continue;
      }

      size_t distance = a.second.history_.back();

      if (distance < current_distance) {
        current_distance = distance;
        to_evict = a.first;
      }
    }
  }

  if (to_evict == -1) {
    return false;
  }

  auto pair = node_store_.find(to_evict);
  node_store_.erase(pair);
  *frame_id = to_evict;
  curr_size_ -= 1;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  //  BUSTUB_ASSERT(static_cast<size_t>(frame_id) >= replacer_size_, "exceed_replacer_size");

  LRUKNode new_node(frame_id, k_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    std::pair<frame_id_t, LRUKNode> entry(frame_id, new_node);

    node_store_.insert(entry);
  }

  auto pair = node_store_.find(frame_id);
  pair->second.AddEntry(CurrentTimestamp());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto frame = node_store_.find(frame_id);
  if (frame == node_store_.end()) {
    return;
  }
  auto is_evictable = frame->second.is_evictable_;
  if (is_evictable && !set_evictable) {
    if (curr_size_ == 1) {
      curr_size_ = 0;
    }  else {
      curr_size_ -= 1;
    }
  } else if (!is_evictable && set_evictable) {
    curr_size_ += 1;
  }

  frame->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto frame = node_store_.find(frame_id);
  if (frame == node_store_.end()) {
    return;
  }

  char buffer[128];
  snprintf(buffer, sizeof(buffer), "frame %d is non-evictable", frame_id);
  BUSTUB_ASSERT(!frame->second.is_evictable_, buffer);
  node_store_.erase(frame_id);
  curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
