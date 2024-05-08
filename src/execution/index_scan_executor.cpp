//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  txn_->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
}

void IndexScanExecutor::Init() {
  key_cursor_ = 0;
  // rid_cursor_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // beginning:
  //   if (rid_cursor_ < rids_.size()) {
  //     *rid = rids_[rid_cursor_++];
  //     auto tuple_with_meta = table_info_->table_->GetTuple(*rid);
  //     if (tuple_with_meta.first.is_deleted_) {
  //       goto beginning;
  //     }
  //     *tuple = tuple_with_meta.second;
  //     return true;
  //   }
  //   rids_.clear();
  //   rid_cursor_ = 0;
  //   while (key_cursor_ < plan_->pred_keys_.size()) {
  //     auto const_expr = dynamic_cast<const ConstantValueExpression *>(plan_->pred_keys_[key_cursor_++].get());
  //     BUSTUB_ASSERT(const_expr != nullptr, "must be constant value expression");
  //     index_info_->index_->ScanKey({std::vector<Value>{const_expr->val_}, &index_info_->key_schema_}, &rids_,
  //                                  exec_ctx_->GetTransaction());
  //     if (!rids_.empty()) {
  //       goto beginning;
  //     }
  //   }
  //   return false;
  while (key_cursor_ < plan_->pred_keys_.size()) {
    auto const_expr = dynamic_cast<const ConstantValueExpression *>(plan_->pred_keys_[key_cursor_++].get());
    BUSTUB_ASSERT(const_expr != nullptr, "must be constant value expression");
    std::vector<RID> rids;
    index_info_->index_->ScanKey({std::vector<Value>{const_expr->val_}, &index_info_->key_schema_}, &rids,
                                 exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(rids.size() <= 1, "duplicated index, not supported");
    if (!rids.empty()) {
      auto current_tuple = table_info_->table_->GetTuple(rids[0]);
      if (current_tuple.first.ts_ == txn_->GetTransactionTempTs()) {
        if (!current_tuple.first.is_deleted_) {
          *rid = rids[0];
          *tuple = current_tuple.second;
          return true;
        }
        continue;
      }
      auto [meta, old_tuple, undo_link] = GetStableTupleAndUndoLink(txn_mgr_, table_info_, rids[0]);
      auto rts = txn_->GetReadTs();
      if (meta.ts_ <= rts) {
        if (!meta.is_deleted_) {
          *rid = rids[0];
          *tuple = old_tuple;
          return true;
        }
      } else {
        std::vector<UndoLog> logs;
        bool found = false;
        if (undo_link.has_value() && undo_link->IsValid()) {
          for (auto log_entry = txn_mgr_->GetUndoLogOptional(*undo_link);;) {
            logs.emplace_back(*log_entry);
            if (log_entry->ts_ <= rts) {
              found = true;
              break;
            }
            if (log_entry->prev_version_.IsValid()) {
              log_entry = txn_mgr_->GetUndoLogOptional(log_entry->prev_version_);
            } else {
              break;
            }
          }
        }
        if (found) {
          auto rebuilt_tuple = ReconstructTuple(&plan_->OutputSchema(), old_tuple, meta, logs);
          if (rebuilt_tuple.has_value()) {
            *tuple = *rebuilt_tuple;
            *rid = rids[0];
            return true;
          }
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
