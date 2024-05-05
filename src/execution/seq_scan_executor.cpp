//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/execution_common.h"
#include "optimizer/optimizer_internal.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  const auto &filter = plan_->filter_predicate_;
  if (filter && IsPredicateConstant(filter)) {
    auto value = filter->Evaluate(nullptr, Schema({}));
    if (!value.IsNull() && !value.GetAs<bool>()) {
      predicate_false_ = true;
      return;
    }
  }
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  table_info_ = catalog->GetTable(oid);
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

void SeqScanExecutor::Init() {
  if (!predicate_false_) {
    iter_.emplace(table_info_->table_->MakeIterator());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (predicate_false_) {
    return false;
  }
  const auto &filter = plan_->filter_predicate_;
  while (!iter_->IsEnd()) {
    auto tuple_with_meta = iter_->GetTuple();
    if (tuple_with_meta.first.ts_ == txn_->GetTransactionTempTs()) {
      if (tuple_with_meta.first.is_deleted_) {
        ++*iter_;
        continue;
      }
      if (filter) {
        auto value = filter->Evaluate(&tuple_with_meta.second, table_info_->schema_);
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++*iter_;
          continue;
        }
      }
      *tuple = tuple_with_meta.second;
      *rid = iter_->GetRID();
      ++*iter_;
      return true;
    }
    auto rts = txn_->GetReadTs();
    if (tuple_with_meta.first.ts_ <= rts) {
      if (tuple_with_meta.first.is_deleted_) {
        ++*iter_;
        continue;
      }
      if (filter) {
        auto value = filter->Evaluate(&tuple_with_meta.second, table_info_->schema_);
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++*iter_;
          continue;
        }
      }
      *tuple = tuple_with_meta.second;
      *rid = iter_->GetRID();
      ++*iter_;
      return true;
    }
    // Build undo log here
    std::vector<UndoLog> logs;
    bool found = false;
    auto log_entry = txn_mgr_->GetUndoLogOptional(*txn_mgr_->GetUndoLink(iter_->GetRID()));
    while (log_entry.has_value()) {
      logs.emplace_back(*log_entry);
      if (log_entry->ts_ <= rts) {
        found = true;
        break;
      }
      log_entry = txn_mgr_->GetUndoLogOptional(log_entry->prev_version_);
    }
    if (found) {
      auto old_tuple = ReconstructTuple(&plan_->OutputSchema(), tuple_with_meta.second, tuple_with_meta.first, logs);
      if (old_tuple.has_value()) {
        if (filter) {
          auto value = filter->Evaluate(&old_tuple.value(), table_info_->schema_);
          if (value.IsNull() || !value.GetAs<bool>()) {
            ++*iter_;
            continue;
          }
        }
        *tuple = *old_tuple;
        *rid = iter_->GetRID();
        ++*iter_;
        return true;
      }
    }
    ++*iter_;
  }
  return false;
}

}  // namespace bustub
