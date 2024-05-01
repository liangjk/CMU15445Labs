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
  return false;
}

}  // namespace bustub
