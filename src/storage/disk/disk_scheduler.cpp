//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  page_shedulers_.reserve(MAX_OUTSTANDING);
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
  for (const auto &pair : page_shedulers_) {
    pair.second->Join();
    delete (pair.second);
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto req = request_queue_.Get();
    if (req.has_value()) {
      auto pid = req->page_id_;
      auto it = page_shedulers_.find(pid);
      if (it != page_shedulers_.end()) {
        it->second->Schedule(std::move(*req), current_++);
      } else {
        AddPageScheduler(pid, new PageScheduler(std::move(*req), disk_manager_, current_++));
      }
    } else {
      for (const auto &pair : page_shedulers_) {
        pair.second->Stop();
      }
      break;
    }
  }
}

void DiskScheduler::AddPageScheduler(page_id_t page_id, PageScheduler *page_scheduler) {
  if (outstanding_ < MAX_OUTSTANDING) {
    outstanding_++;
  } else {
    size_t timestamp = current_;
    std::unordered_map<page_id_t, PageScheduler *>::iterator out;
    for (auto it = page_shedulers_.begin(); it != page_shedulers_.end(); it++) {
      if (it->second->Compare(timestamp)) {
        out = it;
      }
    }
    out->second->Stop();
    out->second->Join();
    delete (out->second);
    page_shedulers_.erase(out);
  }
  page_shedulers_[page_id] = page_scheduler;
}

PageScheduler::PageScheduler(DiskRequest req, DiskManager *disk_manager, size_t timestamp)
    : disk_manager_(disk_manager), timestamp_(timestamp) {
  waiting_.Put(std::move(req));
  running_.emplace([&] { Start(); });
}

void PageScheduler::Start() {
  while (true) {
    auto req = waiting_.Get();
    if (req.has_value()) {
      if (req->is_write_) {
        disk_manager_->WritePage(req->page_id_, req->data_);
      } else {
        disk_manager_->ReadPage(req->page_id_, req->data_);
      }
      req->callback_.set_value(true);
    } else {
      break;
    }
  }
}

void PageScheduler::Schedule(DiskRequest req, size_t timestamp) {
  waiting_.Put(std::move(req));
  timestamp_ = timestamp;
}

auto PageScheduler::Compare(size_t &timestamp) -> bool {
  if (timestamp_ < timestamp) {
    timestamp = timestamp_;
    return true;
  }
  return false;
}

void PageScheduler::Stop() { waiting_.Put(std::nullopt); }

void PageScheduler::Join() {
  if (running_.has_value()) {
    running_->join();
  }
}
}  // namespace bustub
