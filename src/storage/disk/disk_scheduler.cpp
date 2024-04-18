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
  page_schedulers_.reserve(MAX_OUTSTANDING);

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
  for (const auto &pair : page_schedulers_) {
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
      auto it = page_schedulers_.find(pid);
      if (it != page_schedulers_.end()) {
        it->second->Schedule(std::move(*req), current_++);
      } else {
        AddPageScheduler(pid, new PageScheduler(std::move(*req), disk_manager_, current_++));
      }
    } else {
      for (const auto &pair : page_schedulers_) {
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
    for (auto it = page_schedulers_.begin(); it != page_schedulers_.end(); it++) {
      if (it->second->Compare(timestamp)) {
        out = it;
      }
    }
    out->second->Stop();
    out->second->Join();
    delete (out->second);
    page_schedulers_.erase(out);
  }
  page_schedulers_[page_id] = page_scheduler;
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

// #include "common/exception.h"
// #include "storage/disk/disk_manager.h"
// #include "storage/disk/disk_scheduler.h"

// namespace bustub {

// DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
//   page_schedulers_.reserve(MAX_OUTSTANDING);

//   // Spawn the background thread
//   background_thread_.emplace([&] { StartWorkerThread(); });
// }

// DiskScheduler::~DiskScheduler() {
//   // Put a `std::nullopt` in the queue to signal to exit the loop
//   request_queue_.Put(std::nullopt);
//   if (background_thread_.has_value()) {
//     background_thread_->join();
//   }
//   for (const auto &pair : page_schedulers_) {
//     pair.second->Join();
//     delete (pair.second);
//   }
// }

// void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

// void DiskScheduler::StartWorkerThread() {
//   while (true) {
//     auto req = request_queue_.Get();
//     if (req.has_value()) {
//       auto pid = req->page_id_;
//       auto it = page_schedulers_.find(pid);
//       if (it != page_schedulers_.end()) {
//         if (it->second->Join()) {
//           delete (it->second);
//           it->second = new PageScheduler(std::move(*req), disk_manager_, current_++);
//         } else {
//           it->second->Schedule(std::move(*req), current_++);
//         }
//       } else {
//         AddPageScheduler(pid, new PageScheduler(std::move(*req), disk_manager_, current_++));
//       }
//     } else {
//       for (const auto &pair : page_schedulers_) {
//         pair.second->Stop();
//       }
//       break;
//     }
//   }
// }

// void DiskScheduler::AddPageScheduler(page_id_t page_id, PageScheduler *page_scheduler) {
//   outstanding_++;
//   if (outstanding_ >= MAX_OUTSTANDING) {
//     size_t timestamp = current_;
//     std::vector<std::pair<page_id_t, PageScheduler *>> to_delete;
//     for (const auto &pair : page_schedulers_) {
//       if (pair.second->Join()) {
//         to_delete.emplace_back(pair);
//       } else if (pair.second->Compare(timestamp)) {
//         pair.second->Stop();
//       }
//     }
//     for (const auto &pair : to_delete) {
//       page_schedulers_.erase(pair.first);
//       delete (pair.second);
//     }
//     outstanding_ -= to_delete.size();
//   }
//   page_schedulers_[page_id] = page_scheduler;
// }

// PageScheduler::PageScheduler(DiskRequest req, DiskManager *disk_manager, size_t timestamp)
//     : disk_manager_(disk_manager), timestamp_(timestamp) {
//   if (req.is_write_) {
//     writing_ = 1;
//     last_write_ = req.data_;
//   }
//   disk_.Put(std::move(req));
//   back_ = std::thread([&] { Disk(); });
// }

// void PageScheduler::Disk() {
//   while (true) {
//     auto req = disk_.Get();
//     if (req.has_value()) {
//       if (req->is_write_) {
//         disk_manager_->WritePage(req->page_id_, req->data_);
//         if (--writing_ == 0) {
//           latch_.lock();
//           last_write_ = nullptr;
//           latch_.unlock();
//         }
//       } else {
//         disk_manager_->ReadPage(req->page_id_, req->data_);
//       }
//       req->callback_.set_value(true);
//     } else {
//       break;
//     }
//   }
// }

// void PageScheduler::Schedule(DiskRequest req, size_t timestamp) {
//   if (req.is_write_) {
//     ++writing_;
//     last_write_ = req.data_;
//     disk_.Put(std::move(req));
//     timestamp_ = timestamp;
//   } else {
//     latch_.lock();
//     if (last_write_ == nullptr) {
//       latch_.unlock();
//       disk_.Put(std::move(req));
//     } else {
//       memcpy(req.data_, last_write_, BUSTUB_PAGE_SIZE);
//       latch_.unlock();
//       req.callback_.set_value(true);
//     }
//   }
// }

// auto PageScheduler::Compare(size_t &timestamp) -> bool {
//   if (timestamp_ < timestamp) {
//     timestamp = timestamp_;
//     return true;
//   }
//   return false;
// }

// void PageScheduler::Stop() {
//   if (!stop_) {
//     disk_.Put(std::nullopt);
//     stop_ = true;
//   }
// }

// auto PageScheduler::Join() -> bool {
//   if (stop_) {
//     back_.join();
//   }
//   return stop_;
// }
// }  // namespace bustub
