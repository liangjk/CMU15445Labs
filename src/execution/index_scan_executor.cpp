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
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
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
    if (!rids.empty()) {
      auto tuple_with_meta = table_info_->table_->GetTuple(rids[0]);
      if (!tuple_with_meta.first.is_deleted_) {
        *rid = rids[0];
        *tuple = tuple_with_meta.second;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
