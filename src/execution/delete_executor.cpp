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

  std::vector<RID> rids;

  Tuple delete_data;
  RID delete_rid;

  while (child_executor_->Next(&delete_data, &delete_rid)) {
    auto meta = table_ptr->GetTupleMeta(delete_rid);
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict in delete pre-check"));
    }
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

  auto txn_mgr = exec_ctx_->GetTransactionManager();

  size_t deleted = rids.size();
  for (size_t i = 0; i < deleted; ++i) {
    auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_ptr.get(), rids[i]);
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict in delete executing, tainted txn"));
    }
    if (meta.ts_ == txn->GetTransactionTempTs()) {
      if (undo_link.has_value()) {
        BUSTUB_ASSERT(undo_link->prev_txn_ == txn->GetTransactionId(), "tuple must be changed by the same txn");
        auto log = txn->GetUndoLog(undo_link->prev_log_idx_);

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
        size_t partial_idx = 0;
        for (size_t j = 0; j < col_cnt; ++j) {
          if (log.modified_fields_[j]) {
            log_values.emplace_back(log.tuple_.GetValue(&partial_schema, partial_idx++));
          } else {
            log_values.emplace_back(old_tuple.GetValue(&schema, j));
            log.modified_fields_[j] = true;
          }
        }

        log.tuple_ = {std::move(log_values), &schema};

        txn->ModifyUndoLog(undo_link->prev_log_idx_, log);
      }
      if (!table_ptr->UpdateTupleInPlace(delete_meta, delete_tuple, rids[i],
                                         [meta_ref(meta)](const TupleMeta &meta_para, const Tuple &tuple_para,
                                                          RID rid_para) { return meta_para == meta_ref; })) {
        txn->SetTainted();
        throw(ExecutionException("write-write conflict in delete executing, tainted txn"));
      }
    } else {
      UndoLog log{false, std::vector<bool>(col_cnt, true), old_tuple, meta.ts_,
                  undo_link.has_value() ? *undo_link : UndoLink{}};
      auto page_write_guard = table_ptr->AcquireTablePageWriteLock(rids[i]);
      auto page = page_write_guard.AsMut<TablePage>();
      auto base_meta = page->GetTupleMeta(rids[i]);
      if (base_meta != meta) {
        txn->SetTainted();
        throw(ExecutionException("write-write conflict in delete executing, tainted txn"));
      }
      table_ptr->UpdateTupleInPlaceWithLockAcquired(delete_meta, delete_tuple, rids[i], page);
      txn_mgr->UpdateUndoLink(rids[i], txn->AppendUndoLog(log));
      txn->AppendWriteSet(oid, rids[i]);
    }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(deleted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
