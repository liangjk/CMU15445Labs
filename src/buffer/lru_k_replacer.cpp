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
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid, bool evictable, size_t time)
    : history_(1, time), k_(k), fid_(fid), is_evictable_(evictable) {}

void LRUKNode::Access(size_t time) {
  latch_.lock();
  if (history_.size() < k_) {
    history_.push_back(time);
  } else {
    history_.pop_front();
    history_.push_back(time);
  }
  latch_.unlock();
}

auto LRUKNode::Compare(bool &inf, size_t &time) -> bool {
  latch_.lock();
  size_t size = history_.size();
  size_t lra = history_.front();
  latch_.unlock();
  if (inf) {
    if (size >= k_) {
      return false;
    }
    if (lra < time) {
      time = lra;
      return true;
    }
    return false;
  }
  if (size < k_) {
    inf = true;
    time = lra;
    return true;
  }
  if (lra < time) {
    time = lra;
    return true;
  }
  return false;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k) {
  node_store_.reserve(num_frames);
  evictable_.reserve(num_frames);
}

LRUKReplacer::~LRUKReplacer() {
  for (auto pair : node_store_) {
    delete (pair.second);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (evictable_.empty()) {
    latch_.unlock();
    return false;
  }
  bool inf = false;
  size_t time = current_timestamp_;
  LRUKNode *out;
  for (auto node : evictable_) {
    if (node->Compare(inf, time)) {
      out = node;
    }
  }
  evictable_.erase(out);
  node_store_.erase(out->fid_);
  latch_.unlock();
  *frame_id = out->fid_;
  delete (out);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  LRUKNode *node = node_store_[frame_id];
  size_t time = current_timestamp_++;
  if (node == nullptr) {
    node = new LRUKNode(k_, frame_id, false, time);
    node_store_[frame_id] = node;
    latch_.unlock();
    return;
  }
  latch_.unlock();
  node->Access(time);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  LRUKNode *node = node_store_[frame_id];
  if (node == nullptr) {
    latch_.unlock();
    return;
  }
  if (node->is_evictable_ == set_evictable) {
    latch_.unlock();
    return;
  }
  node->is_evictable_ = set_evictable;
  if (set_evictable) {
    evictable_.insert(node);
  } else {
    evictable_.erase(node);
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  LRUKNode *node = node_store_[frame_id];
  if (node == nullptr) {
    latch_.unlock();
    return;
  }
  if (node->is_evictable_) {
    evictable_.erase(node);
  }
  node_store_.erase(frame_id);
  latch_.unlock();
  delete (node);
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t ret = evictable_.size();
  latch_.unlock();
  return ret;
}

}  // namespace bustub
