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
  for (const auto &pair : txn->write_set_) {
    auto table_info = catalog_->GetTable(pair.first);
    const auto &table_heap = table_info->table_;
    const auto &schema = table_info->schema_;
    auto col_cnt = schema.GetColumnCount();
    std::optional<Tuple> empty_tuple{std::nullopt};
    TupleMeta empty_meta{0, true};
    for (const auto &rid : pair.second) {
      auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(this, table_heap.get(), rid);
      BUSTUB_ASSERT(meta.ts_ == txn->txn_id_, "tuple not with correct timestamp");
      if (undo_link.has_value()) {
        BUSTUB_ASSERT(undo_link->prev_txn_ == txn->txn_id_, "tuple was not modified by this txn");
        auto log = txn->GetUndoLog(undo_link->prev_log_idx_);
        BUSTUB_ASSERT(log.prev_version_.prev_txn_ != txn->txn_id_, "txn has more than one undo log entry");

        if (log.is_deleted_) {
          if (!empty_tuple.has_value()) {
            std::vector<Value> empty_values;
            empty_values.reserve(col_cnt);
            for (size_t i = 0; i < col_cnt; ++i) {
              empty_values.emplace_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
            }
            empty_tuple.emplace(std::move(empty_values), &schema);
          }
          TupleMeta new_meta{log.ts_, true};
          auto page_write_guard = table_heap->AcquireTablePageWriteLock(rid);
          auto page = page_write_guard.AsMut<TablePage>();
          table_heap->UpdateTupleInPlaceWithLockAcquired(new_meta, *empty_tuple, rid, page);
          if (log.prev_version_.IsValid()) {
            UpdateUndoLink(rid, log.prev_version_);
          } else {
            UpdateUndoLink(rid, std::nullopt);
          }
        } else {
          std::vector<uint32_t> partial_cols;
          partial_cols.reserve(col_cnt);
          for (size_t i = 0; i < col_cnt; ++i) {
            if (log.modified_fields_[i]) {
              partial_cols.push_back(i);
            }
          }
          auto partial_schema = Schema::CopySchema(&schema, partial_cols);

          std::vector<Value> new_values;
          new_values.reserve(col_cnt);
          size_t partial_idx = 0;
          for (size_t i = 0; i < col_cnt; ++i) {
            if (log.modified_fields_[i]) {
              new_values.emplace_back(log.tuple_.GetValue(&partial_schema, partial_idx++));
            } else {
              new_values.emplace_back(old_tuple.GetValue(&schema, i));
            }
          }
          TupleMeta new_meta{log.ts_, false};

          auto page_write_guard = table_heap->AcquireTablePageWriteLock(rid);
          auto page = page_write_guard.AsMut<TablePage>();
          table_heap->UpdateTupleInPlaceWithLockAcquired(new_meta, Tuple(std::move(new_values), &schema), rid, page);
          if (log.prev_version_.IsValid()) {
            UpdateUndoLink(rid, log.prev_version_);
          } else {
            UpdateUndoLink(rid, std::nullopt);
          }
        }
      } else {
        if (!empty_tuple.has_value()) {
          std::vector<Value> empty_values;
          empty_values.reserve(col_cnt);
          for (size_t i = 0; i < col_cnt; ++i) {
            empty_values.emplace_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
          }
          empty_tuple.emplace(std::move(empty_values), &schema);
        }
        table_heap->UpdateTupleInPlace(empty_meta, *empty_tuple, rid);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
  // txn_map_.erase(txn->txn_id_);
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
