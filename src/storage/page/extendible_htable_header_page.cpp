//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  if (max_depth > HTABLE_HEADER_MAX_DEPTH) {
    return;
  }
  max_depth_ = max_depth;
  max_size_ = 1 << max_depth;
  for (uint32_t i = 0; i < max_size_; ++i) {
    directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  return static_cast<uint64_t>(hash) >> (32 - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> page_id_t {
  if (directory_idx >= max_size_) {
    return INVALID_PAGE_ID;
  }
  return directory_page_ids_[directory_idx];
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageIdSafe(uint32_t directory_idx) const -> page_id_t {
  return directory_page_ids_[directory_idx];
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageIdHash(uint32_t hash) const -> page_id_t {
  return directory_page_ids_[HashToDirectoryIndex(hash)];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  if (directory_idx < max_size_) {
    directory_page_ids_[directory_idx] = directory_page_id;
  }
}

void ExtendibleHTableHeaderPage::SetDirectoryPageIdSafe(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return max_size_; }

}  // namespace bustub
