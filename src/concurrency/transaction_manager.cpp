//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  const auto &write_set = txn->GetWriteSets();
  for (const auto &pair : write_set) {
    auto table = catalog_->GetTable(pair.first);
    for (const auto &rid : pair.second) {
      auto meta = table->table_->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  last_commit_ts_.store(commit_ts);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> new_map;
  for (const auto &pair : txn_map_) {
    if (pair.second->GetTransactionState() < TransactionState::COMMITTED) {
      new_map.insert(pair);
    }
  }
  auto watermark = GetWatermark();
  const auto tables = catalog_->GetTableNames();
  for (const auto &table : tables) {
    auto iter = catalog_->GetTable(table)->table_->MakeIterator();
    while (!iter.IsEnd()) {
      if (iter.GetTuple().first.ts_ <= watermark) {
        ++iter;
        continue;
      }
      auto link_opt = GetUndoLink(iter.GetRID());
      if (link_opt.has_value()) {
        auto link = *link_opt;
        while (link.IsValid()) {
          if (new_map.find(link.prev_txn_) == new_map.end()) {
            new_map[link.prev_txn_] = txn_map_[link.prev_txn_];
          }
          auto log = GetUndoLogOptional(link);
          BUSTUB_ASSERT(log.has_value(), "this entry still alive, should not be empty");
          if (log->ts_ <= watermark) {
            break;
          }
          link = log->prev_version_;
        }
      }
      ++iter;
    }
  }
  txn_map_ = std::move(new_map);
}

}  // namespace bustub
