//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (called_) {
    return false;
  }
  int updated = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);

  auto indexes = catalog->GetTableIndexes(table->name_);

  Tuple old_data;
  RID old_rid;

  const auto &exprs = plan_->target_expressions_;
  std::vector<Value> new_values;
  auto col_cnt = exprs.size();
  new_values.resize(col_cnt);

  auto lock_mgr = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  TupleMeta new_meta{0, false};
  TupleMeta old_meta{0, true};

  const auto &table_ptr = table->table_;
  const auto &schema = table->schema_;

  while (child_executor_->Next(&old_data, &old_rid)) {
    ++updated;
    table_ptr->UpdateTupleMeta(old_meta, old_rid);

    for (size_t i = 0; i < col_cnt; ++i) {
      new_values[i] = exprs[i]->Evaluate(&old_data, schema);
    }
    Tuple new_data{new_values, &schema};

    auto new_rid = table_ptr->InsertTuple(new_meta, new_data, lock_mgr, txn, oid);
    if (new_rid != std::nullopt) [[likely]] {  // NOLINT
      for (const auto &index : indexes) {
        auto key_attr = index->index_->GetKeyAttrs();
        index->index_->DeleteEntry(old_data.KeyFromTuple(schema, index->key_schema_, key_attr), old_rid, txn);
        index->index_->InsertEntry(new_data.KeyFromTuple(schema, index->key_schema_, key_attr), *new_rid, txn);
      }
    } else {
      for (const auto &index : indexes) {
        index->index_->DeleteEntry(old_data.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                                   old_rid, txn);
      }
    }
  }
  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(updated)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
