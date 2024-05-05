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

#include "concurrency/transaction_manager.h"
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

  auto txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = plan_->GetTableOid();
  auto table = catalog->GetTable(oid);
  const auto &table_ptr = table->table_;

  std::vector<Tuple> data;
  std::vector<RID> rids;
  std::vector<TupleMeta> meta;

  Tuple old_data;
  RID old_rid;

  while (child_executor_->Next(&old_data, &old_rid)) {
    meta.emplace_back(table_ptr->GetTupleMeta(old_rid));
    if (meta.back().ts_ > txn->GetReadTs() && meta.back().ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict"));
    }
    data.emplace_back(old_data);
    rids.emplace_back(old_rid);
  }

  const auto &schema = table->schema_;
  auto col_cnt = schema.GetColumnCount();
  const auto &exprs = plan_->target_expressions_;
  TupleMeta new_meta{txn->GetTransactionTempTs(), false};

  auto indexes = catalog->GetTableIndexes(table->name_);

  auto txn_mgr = exec_ctx_->GetTransactionManager();

  size_t updated = data.size();
  for (size_t i = 0; i < updated; ++i) {
    std::vector<Value> new_values;
    std::vector<bool> modified;
    new_values.reserve(col_cnt);
    modified.reserve(col_cnt);
    for (size_t j = 0; j < col_cnt; ++j) {
      new_values.emplace_back(exprs[j]->Evaluate(&data[i], schema));
      modified.push_back(!new_values[j].CompareExactlyEquals(data[i].GetValue(&schema, j)));
    }

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
          }
        }
        auto partial_schema = Schema::CopySchema(&schema, partial_cols);

        std::vector<Value> log_values;
        log_values.reserve(col_cnt);
        std::vector<Column> log_columns;

        size_t cpt_idx = 0;
        for (size_t j = 0; j < col_cnt; ++j) {
          if (log.modified_fields_[j]) {
            log_values.emplace_back(log.tuple_.GetValue(&partial_schema, cpt_idx++));
            log_columns.emplace_back(schema.GetColumn(j));
          } else if (modified[j]) {
            log.modified_fields_[j] = true;
            log_values.emplace_back(data[i].GetValue(&schema, j));
            log_columns.emplace_back(schema.GetColumn(j));
          }
        }
        Schema entry_schema(log_columns);
        log.tuple_ = {std::move(log_values), &entry_schema};
        txn->ModifyUndoLog(link->prev_log_idx_, log);
      }
    } else {
      std::vector<Value> modified_values;
      std::vector<Column> modified_columns;
      modified_values.reserve(col_cnt);
      modified_columns.reserve(col_cnt);
      for (size_t j = 0; j < col_cnt; ++j) {
        if (modified[j]) {
          modified_values.emplace_back(data[i].GetValue(&schema, j));
          modified_columns.emplace_back(schema.GetColumn(j));
        }
      }

      auto link = txn_mgr->GetUndoLink(rids[i]);
      Schema entry_schema(modified_columns);
      UndoLog log{false, std::move(modified), Tuple(std::move(modified_values), &entry_schema), meta[i].ts_,
                  link.has_value() ? *link : UndoLink{}};

      txn_mgr->UpdateUndoLink(rids[i], txn->AppendUndoLog(log));
      txn->AppendWriteSet(oid, rids[i]);
    }
    Tuple new_tuple(std::move(new_values), &schema);
    table_ptr->UpdateTupleInPlace(new_meta, new_tuple, rids[i]);

    for (const auto &index : indexes) {
      auto key_attr = index->index_->GetKeyAttrs();
      index->index_->DeleteEntry(data[i].KeyFromTuple(schema, index->key_schema_, key_attr), rids[i], txn);
      index->index_->InsertEntry(new_tuple.KeyFromTuple(schema, index->key_schema_, key_attr), rids[i], txn);
    }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(updated)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
