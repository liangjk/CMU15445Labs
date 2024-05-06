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

#include "concurrency/transaction_manager.h"
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
          auto meta = table_ptr->GetTupleMeta(scan_res[0]);
          if (!meta.is_deleted_) {
            txn->SetTainted();
            throw(ExecutionException("cannot insert tuple with same index, pre-check"));
          }
          if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
            txn->SetTainted();
            throw(ExecutionException("write-write conflict in insert pre-check"));
          }
        }
        break;
      }
    }
    data.emplace_back(insert_data);
  }

  auto lock_mgr = exec_ctx_->GetLockManager();
  TupleMeta new_meta{txn->GetTransactionTempTs(), false};

  auto txn_mgr = exec_ctx_->GetTransactionManager();

  size_t inserted = data.size();
  for (size_t i = 0; i < inserted; ++i) {
    RID old_rid;
    bool has_old = false;
    for (const auto &index : indexes) {
      if (index->is_primary_key_) {
        std::vector<RID> scan_res;
        index->index_->ScanKey(data[i].KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                               &scan_res, txn);
        if (!scan_res.empty()) {
          has_old = true;
          old_rid = scan_res[0];
        }
        break;
      }
    }
    if (!has_old) {
      auto insert_rid = table_ptr->InsertTuple(new_meta, data[i], lock_mgr, txn, oid);
      BUSTUB_ASSERT(insert_rid.has_value(), "insert failed, what happened?");
      txn->AppendWriteSet(oid, *insert_rid);
      for (const auto &index : indexes) {
        if (!index->index_->InsertEntry(data[i].KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                                        *insert_rid, txn)) {
          txn->SetTainted();
          throw(ExecutionException("index insert failed, tainted txn"));
        }
      }
    } else {
      auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_ptr.get(), old_rid);
      if ((meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) || !meta.is_deleted_) {
        txn->SetTainted();
        throw(ExecutionException("write-write conflict in insert executing, tainted txn"));
      }
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        if (!table_ptr->UpdateTupleInPlace(new_meta, data[i], old_rid,
                                           [meta_ref(meta)](const TupleMeta &meta_para, const Tuple &tuple_para,
                                                            RID rid_para) { return meta_para == meta_ref; })) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in insert executing, tainted txn"));
        }
      } else {
        UndoLog log{true, std::vector<bool>(schema.GetColumnCount(), false), Tuple(), meta.ts_,
                    undo_link.has_value() ? *undo_link : UndoLink{}};
        auto page_write_guard = table_ptr->AcquireTablePageWriteLock(old_rid);
        auto page = page_write_guard.AsMut<TablePage>();
        auto base_meta = page->GetTupleMeta(old_rid);
        if (base_meta != meta) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in insert executing, tainted txn"));
        }
        table_ptr->UpdateTupleInPlaceWithLockAcquired(new_meta, data[i], old_rid, page);
        txn_mgr->UpdateUndoLink(old_rid, txn->AppendUndoLog(log));
        txn->AppendWriteSet(oid, old_rid);
      }
    }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
