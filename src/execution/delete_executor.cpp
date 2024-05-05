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

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (called_) {
    return false;
  }
  auto txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);

  const auto &table_ptr = table->table_;

  std::vector<Tuple> data;
  std::vector<RID> rids;
  std::vector<TupleMeta> meta;

  Tuple delete_data;
  RID delete_rid;

  while (child_executor_->Next(&delete_data, &delete_rid)) {
    meta.emplace_back(table_ptr->GetTupleMeta(delete_rid));
    if (meta.back().ts_ > txn->GetReadTs() && meta.back().ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict"));
    }
    data.emplace_back(delete_data);
    rids.emplace_back(delete_rid);
  }

  const auto &schema = table->schema_;

  TupleMeta delete_meta{txn->GetTransactionTempTs(), true};

  std::vector<Value> delete_values;
  auto col_cnt = schema.GetColumnCount();
  delete_values.reserve(col_cnt);
  for (size_t i = 0; i < col_cnt; ++i) {
    delete_values.emplace_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
  }
  Tuple delete_tuple(std::move(delete_values), &schema);

  // auto indexes = catalog->GetTableIndexes(table->name_);

  auto txn_mgr = exec_ctx_->GetTransactionManager();

  size_t deleted = data.size();
  for (size_t i = 0; i < deleted; ++i) {
    if (meta[i].ts_ == txn->GetTransactionTempTs()) {
      auto link = txn_mgr->GetUndoLink(rids[i]);
      if (link.has_value()) {
        BUSTUB_ASSERT(link->prev_txn_ == txn->GetTransactionId(), "tuple must be changed by the same txn");
        auto log = txn->GetUndoLog(link->prev_log_idx_);

        std::vector<uint32_t> partial_cols;
        partial_cols.reserve(col_cnt);
        for (size_t j = 0; j < col_cnt; ++j) {
          if (log.modified_fields_[j]) {
            partial_cols.push_back(j);
          } else {
            log.modified_fields_[j] = true;
          }
        }
        auto partial_schema = Schema::CopySchema(&schema, partial_cols);

        std::vector<Value> log_values;
        log_values.reserve(col_cnt);
        for (size_t j = 0; j < col_cnt; ++j) {
          log_values.emplace_back(data[i].GetValue(&schema, j));
        }
        for (size_t j = 0; j < partial_cols.size(); ++j) {
          log_values[partial_cols[j]] = log.tuple_.GetValue(&partial_schema, j);
        }

        log.tuple_ = {std::move(log_values), &schema};

        txn->ModifyUndoLog(link->prev_log_idx_, log);
      }
    } else {
      auto link = txn_mgr->GetUndoLink(rids[i]);
      UndoLog log{false, std::vector<bool>(col_cnt, true), data[i], meta[i].ts_, link.has_value() ? *link : UndoLink{}};
      txn_mgr->UpdateUndoLink(rids[i], txn->AppendUndoLog(log));
      txn->AppendWriteSet(oid, rids[i]);
    }
    table_ptr->UpdateTupleInPlace(delete_meta, delete_tuple, rids[i]);
    // for (const auto &index : indexes) {
    //   index->index_->DeleteEntry(data[i].KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
    //                              rids[i], txn);
    // }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(deleted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
