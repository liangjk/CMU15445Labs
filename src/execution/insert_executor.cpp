//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (called_) {
    return false;
  }
  int inserted = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);

  auto indexes = catalog->GetTableIndexes(table->name_);

  Tuple insert_data;
  RID child_rid;
  auto lock_mgr = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  TupleMeta meta{0, false};

  const auto &table_ptr = table->table_;
  const auto &schema = table->schema_;

  while (child_executor_->Next(&insert_data, &child_rid)) {
    auto insert_rid = table_ptr->InsertTuple(meta, insert_data, lock_mgr, txn, oid);
    if (insert_rid != std::nullopt) {
      ++inserted;
      for (const auto &index : indexes) {
        index->index_->InsertEntry(insert_data.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                                   *insert_rid, txn);
      }
    }
  }
  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
