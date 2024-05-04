#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    current_reads_.emplace(read_ts, 1);
    in_map_.push(read_ts);
    if (read_ts < watermark_) {
      watermark_ = read_ts;
    }
  } else {
    ++it->second;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (--current_reads_[read_ts] <= 0) {
    current_reads_.erase(read_ts);
    if (read_ts == in_map_.top()) {
      while (true) {
        in_map_.pop();
        if (in_map_.empty()) {
          watermark_ = commit_ts_;
          break;
        }
        read_ts = in_map_.top();
        if (current_reads_.find(read_ts) != current_reads_.end()) {
          watermark_ = read_ts;
          break;
        }
      }
    }
  }
}

}  // namespace bustub
