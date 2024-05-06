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

  const auto &schema = table->schema_;
  auto col_cnt = schema.GetColumnCount();
  const auto &exprs = plan_->target_expressions_;

  auto indexes = catalog->GetTableIndexes(table->name_);

  std::vector<RID> rids;

  Tuple child_data;
  RID child_rid;

  while (child_executor_->Next(&child_data, &child_rid)) {
    auto meta = table_ptr->GetTupleMeta(child_rid);
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict in update pre-check"));
    }
    for (const auto &index : indexes) {
      if (index->is_primary_key_) {
        auto key_attrs = index->index_->GetKeyAttrs();
        std::vector<Value> new_key_values;
        new_key_values.reserve(key_attrs.size());
        for (auto attr : key_attrs) {
          new_key_values.emplace_back(exprs[attr]->Evaluate(&child_data, schema));
        }

        Tuple new_key(std::move(new_key_values), &index->key_schema_);
        auto old_key = child_data.KeyFromTuple(schema, index->key_schema_, key_attrs);

        if (!IsTupleContentEqual(new_key, old_key)) {
          std::vector<RID> scan_res;
          index->index_->ScanKey(new_key, &scan_res, txn);
          if (!scan_res.empty()) {
            auto meta = table_ptr->GetTupleMeta(scan_res[0]);
            if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
              txn->SetTainted();
              throw(ExecutionException("write-write conflict in update pre-check"));
            }
          }
        }
        break;
      }
    }
    rids.emplace_back(child_rid);
  }

  TupleMeta new_meta{txn->GetTransactionTempTs(), false};

  auto txn_mgr = exec_ctx_->GetTransactionManager();

  size_t updated = rids.size();

  std::vector<Tuple> to_insert;
  to_insert.reserve(updated);

  for (size_t i = 0; i < updated; ++i) {
    auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_ptr.get(), rids[i]);
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw(ExecutionException("write-write conflict in delete executing, tainted txn"));
    }

    std::vector<Value> new_values;
    new_values.reserve(col_cnt);
    for (size_t j = 0; j < col_cnt; ++j) {
      new_values.emplace_back(exprs[j]->Evaluate(&old_tuple, schema));
    }
    Tuple new_tuple(new_values, &schema);

    bool inplace = true;

    for (const auto &index : indexes) {
      if (index->is_primary_key_) {
        auto old_key = old_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        auto new_key = new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        if (!IsTupleContentEqual(old_key, new_key)) {
          inplace = false;
        }
        break;
      }
    }

    if (inplace) {
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
          std::vector<Column> log_columns;
          log_values.reserve(col_cnt);
          log_columns.reserve(col_cnt);
          size_t partial_idx = 0;
          for (size_t j = 0; j < col_cnt; ++j) {
            if (log.modified_fields_[j]) {
              log_values.emplace_back(log.tuple_.GetValue(&partial_schema, partial_idx++));
              log_columns.emplace_back(schema.GetColumn(j));
            } else {
              auto old_val = old_tuple.GetValue(&schema, j);
              if (!new_values[j].CompareExactlyEquals(old_val)) {
                log_values.emplace_back(old_val);
                log_columns.emplace_back(schema.GetColumn(j));
                log.modified_fields_[j] = true;
              }
            }
          }
          Schema log_schema(log_columns);
          log.tuple_ = {std::move(log_values), &log_schema};
          txn->ModifyUndoLog(undo_link->prev_log_idx_, log);
        }
        if (!table_ptr->UpdateTupleInPlace(new_meta, new_tuple, rids[i],
                                           [meta_ref(meta)](const TupleMeta &meta_para, const Tuple &tuple_para,
                                                            RID rid_para) { return meta_para == meta_ref; })) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update executing, tainted txn"));
        }
      } else {
        std::vector<bool> modified;
        std::vector<Value> modified_values;
        std::vector<Column> modified_columns;
        modified_values.reserve(col_cnt);
        modified_columns.reserve(col_cnt);
        for (size_t j = 0; j < col_cnt; ++j) {
          auto old_val = old_tuple.GetValue(&schema, j);
          if (!new_values[j].CompareExactlyEquals(old_val)) {
            modified.push_back(true);
            modified_values.emplace_back(old_val);
            modified_columns.emplace_back(schema.GetColumn(j));
          } else {
            modified.push_back(false);
          }
        }
        Schema log_schema(modified_columns);

        UndoLog log{false, std::move(modified), Tuple(std::move(modified_values), &log_schema), meta.ts_,
                    undo_link.has_value() ? *undo_link : UndoLink{}};
        auto page_write_guard = table_ptr->AcquireTablePageWriteLock(rids[i]);
        auto page = page_write_guard.AsMut<TablePage>();
        auto base_meta = page->GetTupleMeta(rids[i]);
        if (base_meta != meta) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update executing, tainted txn"));
        }
        table_ptr->UpdateTupleInPlaceWithLockAcquired(new_meta, new_tuple, rids[i], page);
        txn_mgr->UpdateUndoLink(rids[i], txn->AppendUndoLog(log));
        txn->AppendWriteSet(oid, rids[i]);
      }
    } else {
      // delete old one
      TupleMeta del_meta{txn->GetTransactionTempTs(), true};

      std::vector<Value> empty_values;
      empty_values.reserve(col_cnt);
      for (size_t i = 0; i < col_cnt; ++i) {
        empty_values.emplace_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
      }
      Tuple del_tuple(std::move(empty_values), &schema);

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
        if (!table_ptr->UpdateTupleInPlace(del_meta, del_tuple, rids[i],
                                           [meta_ref(meta)](const TupleMeta &meta_para, const Tuple &tuple_para,
                                                            RID rid_para) { return meta_para == meta_ref; })) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update-delete executing, tainted txn"));
        }
      } else {
        UndoLog log{false, std::vector<bool>(col_cnt, true), old_tuple, meta.ts_,
                    undo_link.has_value() ? *undo_link : UndoLink{}};
        auto page_write_guard = table_ptr->AcquireTablePageWriteLock(rids[i]);
        auto page = page_write_guard.AsMut<TablePage>();
        auto base_meta = page->GetTupleMeta(rids[i]);
        if (base_meta != meta) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update-delete executing, tainted txn"));
        }
        table_ptr->UpdateTupleInPlaceWithLockAcquired(del_meta, del_tuple, rids[i], page);
        txn_mgr->UpdateUndoLink(rids[i], txn->AppendUndoLog(log));
        txn->AppendWriteSet(oid, rids[i]);
      }
      // insert new one
      to_insert.emplace_back(new_tuple);
    }
  }

  auto lock_mgr = exec_ctx_->GetLockManager();
  for (auto &new_tuple : to_insert) {
    bool update_as_insert = false;
    RID update_as_insert_rid;
    for (const auto &index : indexes) {
      if (index->is_primary_key_) {
        std::vector<RID> scan_res;
        index->index_->ScanKey(new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()),
                               &scan_res, txn);
        if (!scan_res.empty()) {
          update_as_insert = true;
          update_as_insert_rid = scan_res[0];
        }
        break;
      }
    }
    if (!update_as_insert) {
      auto insert_rid = table_ptr->InsertTuple(new_meta, new_tuple, lock_mgr, txn, oid);
      BUSTUB_ASSERT(insert_rid.has_value(), "insert failed, what happened?");
      txn->AppendWriteSet(oid, *insert_rid);
      for (const auto &index : indexes) {
        if (!index->index_->InsertEntry(
                new_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs()), *insert_rid, txn)) {
          txn->SetTainted();
          throw(ExecutionException("index insert failed, tainted txn"));
        }
      }
    } else {
      auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_ptr.get(), update_as_insert_rid);
      if ((meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) || !meta.is_deleted_) {
        txn->SetTainted();
        throw(ExecutionException("write-write conflict in update-insert executing, tainted txn"));
      }
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        if (!table_ptr->UpdateTupleInPlace(new_meta, new_tuple, update_as_insert_rid,
                                           [meta_ref(meta)](const TupleMeta &meta_para, const Tuple &tuple_para,
                                                            RID rid_para) { return meta_para == meta_ref; })) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update-insert executing, tainted txn"));
        }
      } else {
        UndoLog log{true, std::vector<bool>(col_cnt, false), Tuple(), meta.ts_,
                    undo_link.has_value() ? *undo_link : UndoLink{}};
        auto page_write_guard = table_ptr->AcquireTablePageWriteLock(update_as_insert_rid);
        auto page = page_write_guard.AsMut<TablePage>();
        auto base_meta = page->GetTupleMeta(update_as_insert_rid);
        if (base_meta != meta) {
          txn->SetTainted();
          throw(ExecutionException("write-write conflict in update-insert executing, tainted txn"));
        }
        table_ptr->UpdateTupleInPlaceWithLockAcquired(new_meta, new_tuple, update_as_insert_rid, page);
        txn_mgr->UpdateUndoLink(update_as_insert_rid, txn->AppendUndoLog(log));
        txn->AppendWriteSet(oid, update_as_insert_rid);
      }
    }
  }

  *tuple = {std::vector<Value>{ValueFactory::GetIntegerValue(updated)}, &plan_->OutputSchema()};
  called_ = true;
  return true;
}

}  // namespace bustub
