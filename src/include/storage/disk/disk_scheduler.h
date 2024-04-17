//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT
#include <unordered_map>

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

class PageScheduler {
 private:
  DiskManager *disk_manager_;
  Channel<std::optional<DiskRequest>> waiting_;
  std::optional<std::thread> running_;
  size_t timestamp_;
  void Start();

 public:
  PageScheduler(DiskRequest req, DiskManager *disk_manager, size_t timestamp);
  void Schedule(DiskRequest req, size_t timestamp);
  auto Compare(size_t &timestamp) -> bool;
  void Stop();
  void Join();
  ~PageScheduler() = default;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_;
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;

  std::unordered_map<page_id_t, PageScheduler *> page_shedulers_;
  size_t current_{0};
  void AddPageScheduler(page_id_t page_id, PageScheduler *page_scheduler);

  static const int MAX_OUTSTANDING = 128;
  int outstanding_{0};
};
}  // namespace bustub
