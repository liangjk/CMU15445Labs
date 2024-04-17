#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ != nullptr) {
    page_->RLatch();
  }
  return ReadPageGuard(this);
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ != nullptr) {
    page_->WLatch();
  }
  return WritePageGuard(this);
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}

ReadPageGuard::ReadPageGuard(BasicPageGuard *guard) : guard_(std::move(*guard)) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}

WritePageGuard::WritePageGuard(BasicPageGuard *guard) : guard_(std::move(*guard)) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
