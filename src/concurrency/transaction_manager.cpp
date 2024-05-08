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

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->write_set_.empty()) {
    return true;
  }
  auto vfcmt = txn->GetReadTs();
  while (vfcmt < last_commit_ts_) {
    std::vector<const std::unordered_map<table_oid_t, std::unordered_set<RID>> *> to_verify;
    while (vfcmt < last_commit_ts_) {
      const auto &write_set = commit_txns_[vfcmt++]->GetWriteSets();
      if (!write_set.empty()) {
        to_verify.push_back(&write_set);
      }
    }
    if (!to_verify.empty()) {
      commit_mutex_.unlock();
      if (!DoVerify(txn, to_verify)) {
        return false;
      }
      commit_mutex_.lock();
    }
  }
  return true;
}

auto TransactionManager::DoVerify(
    Transaction *txn, const std::vector<const std::unordered_map<table_oid_t, std::unordered_set<RID>> *> &to_verify)
    -> bool {
  std::unordered_map<table_oid_t, std::unordered_set<RID>> verified;
  for (auto write_set : to_verify) {
    for (const auto &pair : *write_set) {
      auto pdit = txn->scan_predicates_.find(pair.first);
      if (pdit != txn->scan_predicates_.end()) {
        auto table_info = catalog_->GetTable(pair.first);
        const auto &schema = table_info->schema_;
        auto vfit = verified.find(pair.first);
        if (vfit == verified.end()) {
          for (const auto &rid : pair.second) {
            auto [meta, now_tuple, undo_link] = GetStableTupleAndUndoLink(this, table_info, rid);
            if (!meta.is_deleted_) {
              for (const auto &expr : pdit->second) {
                auto val = expr->Evaluate(&now_tuple, schema);
                if (val.IsNull() || val.GetAs<bool>()) {
                  return false;
                }
              }
            }
            std::vector<UndoLog> logs;
            bool found = false;
            if (undo_link.has_value()) {
              auto log_entry = GetUndoLogOptional(*undo_link);
              while (log_entry.has_value()) {
                logs.emplace_back(*log_entry);
                if (log_entry->ts_ <= txn->GetReadTs()) {
                  found = true;
                  break;
                }
                log_entry = GetUndoLogOptional(log_entry->prev_version_);
              }
              if (found) {
                auto old_tuple = ReconstructTuple(&schema, now_tuple, meta, logs);
                if (old_tuple.has_value()) {
                  for (const auto &expr : pdit->second) {
                    auto val = expr->Evaluate(&old_tuple.value(), schema);
                    if (val.IsNull() || val.GetAs<bool>()) {
                      return false;
                    }
                  }
                }
              }
            }
          }
          verified[pair.first].insert(pair.second.begin(), pair.second.end());
        } else {
          for (const auto &rid : pair.second) {
            if (vfit->second.find(rid) != vfit->second.end()) {
              continue;
            }
            auto [meta, now_tuple, undo_link] = GetStableTupleAndUndoLink(this, table_info, rid);
            if (!meta.is_deleted_) {
              for (const auto &expr : pdit->second) {
                auto val = expr->Evaluate(&now_tuple, schema);
                if (val.IsNull() || val.GetAs<bool>()) {
                  return false;
                }
              }
            }
            std::vector<UndoLog> logs;
            bool found = false;
            if (undo_link.has_value()) {
              auto log_entry = GetUndoLogOptional(*undo_link);
              while (log_entry.has_value()) {
                logs.emplace_back(*log_entry);
                if (log_entry->ts_ <= txn->GetReadTs()) {
                  found = true;
                  break;
                }
                log_entry = GetUndoLogOptional(log_entry->prev_version_);
              }
              if (found) {
                auto old_tuple = ReconstructTuple(&schema, now_tuple, meta, logs);
                if (old_tuple.has_value()) {
                  for (const auto &expr : pdit->second) {
                    auto val = expr->Evaluate(&old_tuple.value(), schema);
                    if (val.IsNull() || val.GetAs<bool>()) {
                      return false;
                    }
                  }
                }
              }
            }
            vfit->second.insert(rid);
          }
        }
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  commit_mutex_.lock();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      Abort(txn);
      return false;
    }
  }
  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;

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

  // auto &write_set = txn->write_set_;
  // for (auto &pair : write_set) {
  //   auto table_info = catalog_->GetTable(pair.first);
  //   const auto &table = table_info->table_;
  //   std::vector<RID> unchanged;
  //   for (const auto &rid : pair.second) {
  //     auto [meta, now_tuple, undo_link] = GetTupleAndUndoLink(this, table.get(), rid);
  //     if (undo_link.has_value()) {
  //       bool unchange = true;
  //       auto log = GetUndoLog(*undo_link);
  //       if (log.is_deleted_) {
  //         if (!meta.is_deleted_) {
  //           unchange = false;
  //         }
  //       } else {
  //         const auto &schema = table_info->schema_;
  //         auto col_cnt = schema.GetColumnCount();
  //         std::vector<uint32_t> partial_cols;
  //         partial_cols.reserve(col_cnt);
  //         for (size_t i = 0; i < col_cnt; ++i) {
  //           if (log.modified_fields_[i]) {
  //             partial_cols.push_back(i);
  //           }
  //         }
  //         auto partial_schema = Schema::CopySchema(&schema, partial_cols);
  //         for (size_t i = 0; i < partial_cols.size(); ++i) {
  //           if (!log.tuple_.GetValue(&partial_schema, i)
  //                    .CompareExactlyEquals(now_tuple.GetValue(&schema, partial_cols[i]))) {
  //             unchange = false;
  //             break;
  //           }
  //         }
  //       }
  //       if (unchange) {
  //         unchanged.emplace_back(rid);
  //         meta.ts_ = log.ts_;
  //         auto page_write_guard = table->AcquireTablePageWriteLock(rid);
  //         auto page = page_write_guard.AsMut<TablePage>();
  //         page->UpdateTupleMeta(meta, rid);
  //         if (log.prev_version_.IsValid()) {
  //           UpdateUndoLink(rid, log.prev_version_);
  //         } else {
  //           UpdateUndoLink(rid, std::nullopt);
  //         }
  //       } else {
  //         meta.ts_ = commit_ts;
  //         table->UpdateTupleMeta(meta, rid);
  //       }
  //     } else {
  //       if (meta.is_deleted_) {
  //         unchanged.emplace_back(rid);
  //       }
  //       meta.ts_ = commit_ts;
  //       table->UpdateTupleMeta(meta, rid);
  //     }
  //   }
  //   for (const auto &rid : unchanged) {
  //     pair.second.erase(rid);
  //   }
  // }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  last_commit_ts_.store(commit_ts);
  commit_txns_.push_back(txn);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  auto wm = GetWatermark();
  while (cleared_ < wm) {
    ClearTxn(commit_txns_[cleared_++]);
  }

  commit_mutex_.unlock();
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
      auto [meta, old_tuple] = table_heap->GetTuple(rid);
      BUSTUB_ASSERT(meta.ts_ == txn->txn_id_, "tuple not with correct timestamp");
      auto undo_link = GetUndoLink(rid);
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
  ClearTxn(txn);
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
  for (auto &txn : commit_txns_) {
    if (txn != nullptr) {
      if (new_map.find(txn->GetTransactionId()) == new_map.end()) {
        txn = nullptr;
      }
    }
  }
  txn_map_ = std::move(new_map);
}

void TransactionManager::ClearTxn(Transaction *txn) const {
  if (txn != nullptr) {
    txn->undo_logs_.clear();
  }
}

auto GetStableTupleAndUndoLink(TransactionManager *txn_mgr, TableInfo *table_info, RID rid)
    -> std::tuple<TupleMeta, Tuple, std::optional<UndoLink>> {
  auto page_read_guard = table_info->table_->AcquireTablePageReadLock(rid);
  auto page = page_read_guard.As<TablePage>();
  auto [meta, tuple] = page->GetTuple(rid);

  auto undo_link = txn_mgr->GetUndoLink(rid);
  if (meta.ts_ >= TXN_START_ID) {
    auto log = txn_mgr->GetUndoLogOptional(*undo_link);
    page_read_guard.Drop();
    if (log.has_value()) {
      TupleMeta new_meta{log->ts_, log->is_deleted_};
      const auto &schema = table_info->schema_;
      auto col_cnt = schema.GetColumnCount();
      std::vector<Value> new_values;
      new_values.reserve(col_cnt);
      if (log->is_deleted_) {
        for (size_t i = 0; i < col_cnt; ++i) {
          new_values.emplace_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
        }
      } else {
        std::vector<uint32_t> partial_cols;
        partial_cols.reserve(col_cnt);
        for (size_t i = 0; i < col_cnt; ++i) {
          if (log->modified_fields_[i]) {
            partial_cols.push_back(i);
          }
        }
        auto partial_schema = Schema::CopySchema(&schema, partial_cols);

        size_t partial_idx = 0;
        for (size_t i = 0; i < col_cnt; ++i) {
          if (log->modified_fields_[i]) {
            new_values.emplace_back(log->tuple_.GetValue(&partial_schema, partial_idx++));
          } else {
            new_values.emplace_back(tuple.GetValue(&schema, i));
          }
        }
      }

      return std::make_tuple(new_meta, Tuple(std::move(new_values), &schema), log->prev_version_);
    }
    return std::make_tuple(TupleMeta{0, true}, Tuple(), std::nullopt);
  }
  return std::make_tuple(meta, tuple, undo_link);
}

}  // namespace bustub
