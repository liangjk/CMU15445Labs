//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (called_) {
    return false;
  }
  int deleted = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);

  auto indexes = catalog->GetTableIndexes(table->name_);

  Tuple delete_data;
  RID delete_rid;
  TupleMeta delete_meta{0, true};
  auto txn = exec_ctx_->GetTransaction();

  const auto &table_ptr = table->table_;
  const auto &schema = table->schema_;

  while (child_executor_->Next(&delete_data, &delete_rid)) {
    table_ptr->UpdateTupleMeta(delete_meta, delete_rid);
    ++deleted;
    for (const auto &index : indexes) {
      index->index_->DeleteEntry(delete_data.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                                 delete_rid, txn);
    }
  }
  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(deleted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
