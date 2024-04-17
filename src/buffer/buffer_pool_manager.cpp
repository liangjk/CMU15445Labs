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
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  page_locks_ = new std::mutex[pool_size_];
  page_ready_ = new bool[pool_size_];
  page_cvs_ = new std::condition_variable[pool_size_];

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] page_locks_;
  delete[] page_ready_;
  delete[] page_cvs_;
}

auto BufferPoolManager::WriteBack(Page *page) -> std::thread * {
  std::unique_lock<std::mutex> lock(wb_lock_);
  wb_cv_.wait(lock, [&] { return wb_count_ < WB_SIZE; });
  wb_count_++;
  Page *cache = write_back_cache_.Add(page);
  cache->page_id_ = page->GetPageId();
  return new std::thread(
      [&](Page *page_cache) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule({true, page_cache->GetData(), page_cache->GetPageId(), std::move(promise)});
        future.get();
        wb_lock_.lock();
        write_back_cache_.Remove(page_cache);
        wb_count_--;
        wb_lock_.unlock();
        wb_cv_.notify_one();
      },
      cache);
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
    page_id_t pid = AllocatePage();
    page_table_[pid] = fid;
    replacer_->SetEvictable(fid, false);
    latch_.unlock();
    page_ready_[fid] = true;
    Page *pg = pages_ + fid;
    pg->page_id_ = pid;
    pg->is_dirty_ = false;
    pg->pin_count_ = 1;
    pg->ResetMemory();
    *page_id = pid;
    return pg;
  }
  if (!replacer_->Evict(&fid)) {
    latch_.unlock();
    return nullptr;
  }
  replacer_->SetEvictable(fid, false);
  Page *pg = pages_ + fid;
  page_table_.erase(pg->GetPageId());
  page_id_t pid = AllocatePage();
  page_table_[pid] = fid;
  latch_.unlock();
  if (pg->IsDirty()) {
    auto thread = WriteBack(pg);
    thread->detach();
    delete thread;
  }
  pg->page_id_ = pid;
  pg->is_dirty_ = false;
  pg->pin_count_ = 1;
  pg->ResetMemory();
  *page_id = pid;
  page_ready_[fid] = true;
  return pg;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  latch_.lock();
  frame_id_t fid;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    fid = it->second;
    Page *pg = pages_ + fid;
    if (pg->pin_count_ == 0) {
      replacer_->SetEvictable(fid, false);
    }
    pg->pin_count_++;
    latch_.unlock();
    std::unique_lock<std::mutex> lock(page_locks_[fid]);
    page_cvs_[fid].wait(lock, [&] { return page_ready_[fid]; });
    lock.unlock();
    replacer_->RecordAccess(fid, access_type);
    return pg;
  }
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
    page_table_[page_id] = fid;
    replacer_->SetEvictable(fid, false);
    page_ready_[fid] = false;
    Page *pg = pages_ + fid;
    pg->pin_count_ = 1;
    latch_.unlock();
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pg->GetData(), page_id, std::move(promise)});
    pg->page_id_ = page_id;
    pg->is_dirty_ = false;
    future.get();
    page_locks_[fid].lock();
    page_ready_[fid] = true;
    page_locks_[fid].unlock();
    page_cvs_[fid].notify_all();
    return pg;
  }
  if (!replacer_->Evict(&fid)) {
    latch_.unlock();
    return nullptr;
  }
  replacer_->SetEvictable(fid, false);
  page_ready_[fid] = false;
  Page *pg = pages_ + fid;
  page_table_.erase(pg->GetPageId());
  page_table_[page_id] = fid;
  pg->pin_count_ = 1;
  latch_.unlock();
  if (pg->IsDirty()) {
    auto thread = WriteBack(pg);
    thread->detach();
    delete thread;
  }
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pg->GetData(), page_id, std::move(promise)});
  pg->page_id_ = page_id;
  pg->is_dirty_ = false;
  future.get();
  page_locks_[fid].lock();
  page_ready_[fid] = true;
  page_locks_[fid].unlock();
  page_cvs_[fid].notify_all();
  return pg;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t fid = it->second;
  Page *pg = pages_ + fid;
  if (pg->GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }
  page_locks_[fid].lock();
  if (!page_ready_[fid]) {
    page_locks_[fid].unlock();
    latch_.unlock();
    return false;
  }
  page_locks_[fid].unlock();
  pg->is_dirty_ |= is_dirty;
  pg->pin_count_--;
  if (pg->pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t fid = it->second;
  page_locks_[fid].lock();
  if (!page_ready_[fid]) {
    page_locks_[fid].unlock();
    latch_.unlock();
    return false;
  }
  page_locks_[fid].unlock();
  Page *pg = pages_ + fid;
  pg->is_dirty_ = false;
  auto thread = WriteBack(pg);
  latch_.unlock();
  thread->join();
  delete thread;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  std::vector<std::future<bool>> futures;
  futures.reserve(page_table_.size());
  for (const auto &pair : page_table_) {
    frame_id_t fid = pair.second;
    page_locks_[fid].lock();
    if (!page_ready_[fid]) {
      page_locks_[fid].unlock();
      continue;
    }
    page_locks_[fid].unlock();
    Page *pg = pages_ + fid;
    auto promise = disk_scheduler_->CreatePromise();
    futures.push_back(promise.get_future());
    disk_scheduler_->Schedule({true, pg->GetData(), pair.first, std::move(promise)});
    pg->is_dirty_ = false;
  }
  for (auto &future : futures) {
    future.get();
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    DeallocatePage(page_id);
    latch_.unlock();
    return true;
  }
  frame_id_t fid = it->second;
  Page *pg = pages_ + fid;
  if (pg->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(fid);
  free_list_.push_back(fid);
  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
