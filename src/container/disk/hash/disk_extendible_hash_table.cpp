//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto header_page = bpm_->NewPage(&header_page_id_);
  auto header_obj = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page->GetData());
  header_obj->Init(header_max_depth);
  header_pinned_ = true;
}

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::~DiskExtendibleHashTable() {
  UnpinHeader();
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UnpinHeader() {
  if (header_pinned_) {
    bpm_->UnpinPage(header_page_id_, true);
    header_pinned_ = false;
  }
}

// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::UnpinDirectory() {
//   if (directory_pinned_) {
//     auto header_page = bpm_->FetchPage(header_page_id_);
//     auto header_obj = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page->GetData());
//     header_page->RLatch();
//     uint32_t header_size = 1 << header_max_depth_;
//     for (uint32_t i = 0; i < header_size; ++i) {
//       auto dir_pid = header_obj->GetDirectoryPageIdSafe(i);
//       if (dir_pid != INVALID_PAGE_ID) {
//         bpm_->UnpinPage(dir_pid, true);
//       }
//     }
//     header_page->RUnlatch();
//     bpm_->UnpinPage(header_page_id_, true);
//     bpm_->UnpinPage(header_page_id_, true);
//     directory_pinned_ = false;
//   }
// }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  page_id_t dir_pid;
  auto hash = Hash(key);
  {
    auto page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = page_guard.As<ExtendibleHTableHeaderPage>();
    dir_pid = header_page->GetDirectoryPageIdHash(hash);
  }
  if (dir_pid != INVALID_PAGE_ID) {
    auto dir_page_guard = bpm_->FetchPageRead(dir_pid);
    auto dir_page = dir_page_guard.As<ExtendibleHTableDirectoryPage>();
    auto bucket_pid = dir_page->GetBucketPageIdHash(hash);
    auto bucket_page_guard = bpm_->FetchPageRead(bucket_pid);
    auto bucket_page = bucket_page_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    V value;
    if (bucket_page->Lookup(key, value, cmp_)) {
      result->push_back(value);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  page_id_t dir_pid;
  page_id_t bucket_pid;
  Page *dir_page;
  Page *bucket_page;
  ExtendibleHTableDirectoryPage *dir_obj;
  ExtendibleHTableBucketPage<K, V, KC> *bucket_obj;
  auto hash = Hash(key);

  auto header_page = bpm_->FetchPage(header_page_id_);
  auto header_obj = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page->GetData());
  header_page->RLatch();
  dir_pid = header_obj->GetDirectoryPageIdHash(hash);
  header_page->RUnlatch();
  if (dir_pid == INVALID_PAGE_ID) {
    header_page->WLatch();
    NewDirectory(header_obj, hash, dir_pid, dir_page, dir_obj, bucket_pid, bucket_page, bucket_obj);
    header_page->WUnlatch();
    bpm_->UnpinPage(header_page_id_, true);
  } else {
    bpm_->UnpinPage(header_page_id_, false);
    dir_page = bpm_->FetchPage(dir_pid);
    dir_obj = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page->GetData());
    dir_page->RLatch();
    bucket_pid = dir_obj->GetBucketPageIdHash(hash);
    bucket_page = bpm_->FetchPage(bucket_pid);
    bucket_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page->GetData());
    bucket_page->WLatch();
  }

  uint32_t bucket_idx;
  // holding dir_page Read lock, bucket_page Write lock and pin both pages here
  {
    auto end = [&](bool bucket_dirty) {
      bucket_page->WUnlatch();
      bpm_->UnpinPage(bucket_pid, bucket_dirty);
      dir_page->RUnlatch();
      bpm_->UnpinPage(dir_pid, false);
    };

    if (bucket_obj->Find(key, cmp_, bucket_idx)) {
      end(false);
      return false;
    }
    if (!bucket_obj->IsFull()) {
      bucket_obj->SafeInsert(key, value);
      end(true);
      return true;
    }
  }

  // need split bucket here
  bucket_page->WUnlatch();
  dir_page->RUnlatch();
  dir_page->WLatch();
  uint32_t dir_idx = dir_obj->HashToBucketIndex(hash);
  auto new_bucket = dir_obj->GetBucketPageIdSafe(dir_idx);
  if (new_bucket != bucket_pid) [[unlikely]] {  // NOLINT
    bpm_->UnpinPage(bucket_pid, false);
    bucket_pid = new_bucket;
    bucket_page = bpm_->FetchPage(bucket_pid);
    bucket_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page->GetData());
  }

  // holding dir_page Write lock, no need for bucket_page lock
  {
    auto end = [&](bool dir_dirty, bool bucket_dirty) {
      bpm_->UnpinPage(bucket_pid, bucket_dirty);
      dir_page->WUnlatch();
      bpm_->UnpinPage(dir_pid, dir_dirty);
    };

    if (bucket_obj->Find(key, cmp_, bucket_idx)) [[unlikely]] {  // NOLINT
      end(false, false);
      return false;
    }

    if (!bucket_obj->IsFull()) [[unlikely]] {  // NOLINT
      bucket_obj->SafeInsert(key, value);
      end(false, true);
      return true;
    }

    uint32_t hashes[bucket_max_size_];
    uint32_t hash_diff = 0;
    hashes[0] = Hash(bucket_obj->KeyAt(0));
    for (uint32_t i = 1; i < bucket_max_size_; ++i) {
      hashes[i] = Hash(bucket_obj->KeyAt(i));
      hash_diff |= hashes[i] ^ hashes[0];
    }

    if ((((hash ^ hashes[0]) | hash_diff) & ((1 << directory_max_depth_) - 1)) != 0) [[likely]] {  // NOLINT
      bool bucket_dirty = false;
      uint8_t local_depth = dir_obj->GetLocalDepthSafe(dir_idx);
      uint32_t bit_flag = 1 << local_depth;
      do {
        page_id_t split_pid;
        auto split_page = bpm_->NewPage(&split_pid);
        if (split_page == nullptr) [[unlikely]] {  // NOLINT
          UnpinHeader();
          split_page = bpm_->NewPage(&split_pid);
        }
        auto split_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(split_page->GetData());
        split_obj->Init(bucket_max_size_);
        if ((hash_diff & bit_flag) != 0) {
          MigrateEntries(bucket_obj, split_obj, bit_flag, hashes);
          bucket_dirty = true;
          dir_obj->Split(dir_idx, split_pid, true);
        } else {
          dir_obj->Split(dir_idx, split_pid, (hashes[0] & bit_flag) == 0);
        }
        dir_idx = dir_obj->HashToBucketIndex(hash);
        new_bucket = dir_obj->GetBucketPageIdSafe(dir_idx);
        if (new_bucket == split_pid) {
          bpm_->UnpinPage(bucket_pid, bucket_dirty);
          bucket_pid = split_pid;
          bucket_page = split_page;
          bucket_obj = split_obj;
        } else {
          bpm_->UnpinPage(split_pid, true);
        }
        bit_flag <<= 1;
      } while (bucket_obj->IsFull());

      bucket_obj->SafeInsert(key, value);
      bpm_->UnpinPage(bucket_pid, true);
      dir_page->WUnlatch();
      bpm_->UnpinPage(dir_pid, true);
      return true;
    }

    end(false, false);
  }

  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::NewDirectory(ExtendibleHTableHeaderPage *header, uint32_t hash,
                                                     page_id_t &dir_pid, Page *&dir_page,
                                                     ExtendibleHTableDirectoryPage *&dir_obj, page_id_t &bucket_pid,
                                                     Page *&bucket_page,
                                                     ExtendibleHTableBucketPage<K, V, KC> *&bucket_obj) {
  auto header_idx = header->HashToDirectoryIndex(hash);
  dir_pid = header->GetDirectoryPageIdSafe(header_idx);
  if (dir_pid == INVALID_PAGE_ID) [[likely]] {  // NOLINT
    dir_page = bpm_->NewPage(&dir_pid);
    bpm_->SetDirty(dir_page, true);
    // if (directory_pinned_) {
    //   bpm_->AddPinCount(dir_page, 1);
    // }
    dir_obj = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page->GetData());
    dir_obj->Init(directory_max_depth_);

    header->SetDirectoryPageIdSafe(header_idx, dir_pid);

    bucket_page = bpm_->NewPage(&bucket_pid);
    bucket_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page->GetData());
    bucket_obj->Init(bucket_max_size_);

    dir_obj->SetBucketPageIdSafe(0, bucket_pid);

    dir_page->RLatch();
    bucket_page->WLatch();
  } else {
    dir_page = bpm_->FetchPage(dir_pid);
    dir_obj = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page->GetData());
    dir_page->RLatch();
    bucket_pid = dir_obj->GetBucketPageIdHash(hash);
    bucket_page = bpm_->FetchPage(bucket_pid);
    bucket_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page->GetData());
    bucket_page->WLatch();
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t bit_flag, uint32_t hashes[]) {
  uint32_t old_size = bucket_max_size_;
  uint32_t index = 0;
  while (index < old_size) {
    if ((hashes[index] & bit_flag) != 0) {
      new_bucket->InsertPair(old_bucket->PairAt(index));
      old_bucket->RemoveAt(index);
      --old_size;
      hashes[index] = hashes[old_size];
    } else {
      ++index;
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  auto header_page = bpm_->FetchPage(header_page_id_);
  auto header_obj = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page->GetData());
  header_page->RLatch();
  auto dir_pid = header_obj->GetDirectoryPageIdHash(hash);
  header_page->RUnlatch();
  bpm_->UnpinPage(header_page_id_, false);
  if (dir_pid == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_page = bpm_->FetchPage(dir_pid);
  auto dir_obj = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page->GetData());
  dir_page->RLatch();
  auto dir_idx = dir_obj->HashToBucketIndex(hash);
  auto bucket_pid = dir_obj->GetBucketPageIdSafe(dir_idx);
  auto bucket_page = bpm_->FetchPage(bucket_pid);
  auto bucket_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page->GetData());
  bucket_page->WLatch();

  {
    auto end = [&](bool bucket_dirty) {
      bucket_page->WUnlatch();
      bpm_->UnpinPage(bucket_pid, bucket_dirty);
      dir_page->RUnlatch();
      bpm_->UnpinPage(dir_pid, false);
    };

    uint32_t bucket_idx;
    if (!bucket_obj->Find(key, cmp_, bucket_idx)) {
      end(false);
      return false;
    }

    bucket_obj->RemoveAt(bucket_idx);
    if (!bucket_obj->IsEmpty()) {
      end(true);
      return true;
    }

    auto bucket_local_depth = dir_obj->GetLocalDepthSafe(dir_idx);

    if (bucket_local_depth == 0) [[unlikely]] {  // NOLINT
      end(true);
      return true;
    }

    auto split_idx = dir_obj->GetSplitImageIndex(dir_idx);
    auto split_local_depth = dir_obj->GetLocalDepthSafe(split_idx);

    if (bucket_local_depth != split_local_depth) {
      end(true);
      return true;
    }
  }
  {
    bpm_->SetDirty(bucket_page, true);
    bucket_page->WUnlatch();
    dir_page->RUnlatch();
    dir_page->WLatch();
    auto end = [&](bool dir_dirty) {
      dir_page->WUnlatch();
      bpm_->UnpinPage(bucket_pid, false);
      bpm_->UnpinPage(dir_pid, dir_dirty);
    };
    if (!bucket_obj->IsEmpty()) {
      end(false);
      return true;
    }

    auto new_idx = dir_obj->HashToBucketIndex(hash);
    if (new_idx != dir_idx) [[unlikely]] {  // NOLINT
      end(false);
      return true;
    }
    auto new_pid = dir_obj->GetBucketPageIdSafe(new_idx);
    if (new_pid != bucket_pid) [[unlikely]] {  // NOLINT
      end(false);
      bpm_->DeletePage(bucket_pid);
      return true;
    }

    auto old_depth = dir_obj->GetLocalDepthSafe(dir_idx);
    auto new_depth = old_depth;
    bool bucket_empty = true;

    for (; new_depth > 0; --new_depth) {
      auto split_idx = dir_obj->GetSplitImageIndex(dir_idx);
      auto split_local_depth = dir_obj->GetLocalDepthSafe(split_idx);
      if (new_depth != split_local_depth) {
        break;
      }
      auto split_pid = dir_obj->GetBucketPageIdSafe(split_idx);
      auto split_page = bpm_->FetchPage(split_pid);
      auto split_obj = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(split_page->GetData());
      if (!split_obj->IsEmpty()) {
        if (!bucket_empty) {
          bpm_->UnpinPage(split_pid, false);
          break;
        }
        dir_obj->Merge(dir_idx, split_idx, false, true);
        bpm_->UnpinPage(bucket_pid, false);
        bpm_->DeletePage(bucket_pid);
        bucket_pid = split_pid;
        bucket_empty = false;
      } else if (dir_obj->Merge(dir_idx, split_idx, !bucket_empty, false)) {
        bpm_->UnpinPage(bucket_pid, false);
        bpm_->DeletePage(bucket_pid);
        bucket_pid = split_pid;
      } else {
        bpm_->UnpinPage(split_pid, false);
        bpm_->DeletePage(split_pid);
      }
    }

    if (old_depth == dir_obj->GetGlobalDepth() && new_depth < old_depth) {
      dir_obj->TryShrink();
    }
    end(true);
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
