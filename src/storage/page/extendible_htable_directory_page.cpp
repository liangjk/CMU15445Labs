//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  if (max_depth > HTABLE_DIRECTORY_MAX_DEPTH) {
    return;
  }
  max_depth_ = max_depth;
  max_size_ = 1 << max_depth;
  global_depth_ = 0;
  current_size_ = 1;
  local_depths_[0] = 0;
  bucket_page_ids_[0] = INVALID_PAGE_ID;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return current_size_ - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= current_size_) {
    return 0;
  }
  return (1 << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= current_size_) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetBucketPageIdSafe(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetBucketPageIdHash(uint32_t hash) const -> page_id_t {
  return bucket_page_ids_[HashToBucketIndex(hash)];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx < current_size_) {
    bucket_page_ids_[bucket_idx] = bucket_page_id;
  }
}

void ExtendibleHTableDirectoryPage::SetBucketPageIdSafe(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ < max_depth_) {
    memcpy(local_depths_ + current_size_, local_depths_, sizeof(uint8_t) << global_depth_);
    memcpy(bucket_page_ids_ + current_size_, bucket_page_ids_, sizeof(page_id_t) << global_depth_);
    ++global_depth_;
    current_size_ <<= 1;
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ > 0) {
    --global_depth_;
    current_size_ >>= 1;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < current_size_; ++i) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

void ExtendibleHTableDirectoryPage::TryShrink() {
  uint8_t max_local_depth = 0;
  for (uint32_t i = 0; i < current_size_; ++i) {
    if (local_depths_[i] > max_local_depth) {
      max_local_depth = local_depths_[i];
    }
  }
  if (max_local_depth < global_depth_) {
    global_depth_ = max_local_depth;
    current_size_ = 1 << global_depth_;
  }
}

auto ExtendibleHTableDirectoryPage::Merge(uint32_t bucket_idx, uint32_t split_idx, bool must_use_split) -> bool {
  auto local_depth = local_depths_[bucket_idx];
  auto local_size = 1 << local_depth;
  auto local_depth_mask = local_size - 1;
  auto bucket_start = bucket_idx & local_depth_mask;
  auto split_start = split_idx & local_depth_mask;
  bool use_split = must_use_split || (split_start > bucket_start);
  if (use_split) {
    auto new_pid = bucket_page_ids_[split_idx];
    for (auto i = bucket_start; i < current_size_; i += local_size) {
      local_depths_[i] = local_depth - 1;
      bucket_page_ids_[i] = new_pid;
    }
    for (auto i = split_start; i < current_size_; i += local_size) {
      local_depths_[i] = local_depth - 1;
    }
  } else {
    auto new_pid = bucket_page_ids_[bucket_idx];
    for (auto i = bucket_start; i < current_size_; i += local_size) {
      local_depths_[i] = local_depth - 1;
    }
    for (auto i = split_start; i < current_size_; i += local_size) {
      local_depths_[i] = local_depth - 1;
      bucket_page_ids_[i] = new_pid;
    }
  }
  return use_split;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return current_size_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return max_size_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint8_t {
  if (bucket_idx >= current_size_) {
    return 0;
  }
  return local_depths_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthSafe(uint32_t bucket_idx) const -> uint8_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx < current_size_) {
    local_depths_[bucket_idx] = local_depth;
  }
}

void ExtendibleHTableDirectoryPage::Split(uint32_t bucket_idx, page_id_t split_page_id, bool stay) {
  page_id_t former;
  page_id_t latter;
  if (stay) {
    former = bucket_page_ids_[bucket_idx];
    latter = split_page_id;
  } else {
    former = split_page_id;
    latter = bucket_page_ids_[bucket_idx];
  }
  auto local_depth = local_depths_[bucket_idx];
  if (local_depth < global_depth_) {
    uint32_t border = 1 << (global_depth_ - 1);
    uint32_t step = 1 << local_depth;
    for (; bucket_idx < border; bucket_idx += step) {
      bucket_page_ids_[bucket_idx] = former;
      local_depths_[bucket_idx] = local_depth + 1;
    }
    border <<= 1;
    for (; bucket_idx < border; bucket_idx += step) {
      bucket_page_ids_[bucket_idx] = latter;
      local_depths_[bucket_idx] = local_depth + 1;
    }
    return;
  }
  bucket_page_ids_[bucket_idx] = former;
  local_depths_[bucket_idx] = local_depth + 1;
  bucket_idx |= 1 << global_depth_;
  IncrGlobalDepth();
  bucket_page_ids_[bucket_idx] = latter;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx < current_size_ && local_depths_[bucket_idx] < max_depth_) {
    ++local_depths_[bucket_idx];
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx < current_size_ && local_depths_[bucket_idx] > 0) {
    --local_depths_[bucket_idx];
  }
}

}  // namespace bustub
