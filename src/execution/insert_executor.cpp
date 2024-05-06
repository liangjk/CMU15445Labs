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

void InsertExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (called_) {
    return false;
  }
  auto txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);

  const auto &table_ptr = table->table_;

  std::vector<Tuple> data;

  auto indexes = catalog->GetTableIndexes(table->name_);
  const auto &schema = table->schema_;

  Tuple insert_data;
  RID child_rid;

  while (child_executor_->Next(&insert_data, &child_rid)) {
    for (const auto &index : indexes) {
      if (index->is_primary_key_) {
        std::vector<RID> scan_res;
        index->index_->ScanKey(insert_data.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                               &scan_res, txn);
        if (!scan_res.empty()) {
          txn->SetTainted();
          throw(ExecutionException("cannot insert a tuple with same primary key"));
        }
      }
    }
    data.emplace_back(insert_data);
  }

  auto lock_mgr = exec_ctx_->GetLockManager();
  TupleMeta meta{txn->GetTransactionTempTs(), false};

  size_t inserted = data.size();
  for (size_t i = 0; i < inserted; ++i) {
    auto insert_rid = table_ptr->InsertTuple(meta, data[i], lock_mgr, txn, oid);
    BUSTUB_ASSERT(insert_rid.has_value(), "insert failed, what happened?");
    txn->AppendWriteSet(oid, *insert_rid);
    for (const auto &index : indexes) {
      if (!index->index_->InsertEntry(
              insert_data.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()), *insert_rid, txn)) {
        txn->SetTainted();
        throw(ExecutionException("cannot insert a tuple because of concurrency, tainted txn"));
      };
    }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
