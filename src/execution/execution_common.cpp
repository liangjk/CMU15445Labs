#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty()) {
    if (base_meta.is_deleted_) {
      return std::nullopt;
    }
    return std::make_optional(base_tuple);
  }
  if (undo_logs.rbegin()->is_deleted_) {
    return std::nullopt;
  }

  std::vector<Value> values;
  std::vector<bool> flags;
  auto col_cnt = schema->GetColumnCount();
  size_t flags_set = 0;
  values.resize(col_cnt);
  flags.resize(col_cnt);

  for (auto it = undo_logs.crbegin(); it != undo_logs.crend(); ++it) {
    bool valid = false;
    for (size_t i = 0; i < col_cnt; ++i) {
      if (it->modified_fields_[i] && !flags[i]) {
        valid = true;
        break;
      }
    }
    if (valid) {
      std::vector<uint32_t> partial_cols;
      partial_cols.reserve(col_cnt);
      for (size_t i = 0; i < col_cnt; ++i) {
        if (it->modified_fields_[i]) {
          partial_cols.push_back(i);
        }
      }
      auto partial_schema = Schema::CopySchema(schema, partial_cols);
      auto sz = partial_cols.size();
      for (size_t i = 0; i < sz; ++i) {
        auto col = partial_cols[i];
        if (!flags[col]) {
          flags[col] = true;
          ++flags_set;
          values[col] = it->tuple_.GetValue(&partial_schema, i);
        }
      }
      if (flags_set == col_cnt) {
        break;
      }
    }
  }
  if (flags_set < col_cnt) {
    for (size_t i = 0; i < col_cnt; ++i) {
      if (!flags[i]) {
        values[i] = base_tuple.GetValue(schema, i);
      }
    }
  }

  return std::make_optional<Tuple>(std::move(values), schema);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    fmt::print("RID={}/{} ts=", iter.GetRID().GetPageId(), iter.GetRID().GetSlotNum());
    auto tuple_pair = iter.GetTuple();
    auto &meta = tuple_pair.first;
    if (meta.ts_ >= TXN_START_ID) {
      fmt::print("txn{} ", meta.ts_ ^ TXN_START_ID);
    } else {
      fmt::print("{} ", meta.ts_);
    }
    if (meta.is_deleted_) {
      fmt::print("<del marker> ");
    }
    fmt::println("tuple={}", tuple_pair.second.ToString(&table_info->schema_));
    auto old = txn_mgr->GetUndoLink(iter.GetRID());
    if (old.has_value()) {
      auto link = *old;
      while (link.IsValid()) {
        auto log = txn_mgr->GetUndoLogOptional(link);
        if (log.has_value()) {
          std::string log_string;
          if (log->is_deleted_) {
            log_string = "<del>";
          } else {
            std::vector<uint32_t> partial_cols;
            auto col_cnt = table_info->schema_.GetColumnCount();
            partial_cols.reserve(col_cnt);
            for (size_t i = 0; i < col_cnt; ++i) {
              if (log->modified_fields_[i]) {
                partial_cols.push_back(i);
              }
            }
            auto partial_schema = Schema::CopySchema(&table_info->schema_, partial_cols);
            log_string = "(";
            size_t new_idx = 0;
            for (size_t i = 0; i < col_cnt; ++i) {
              if (i > 0) {
                log_string += ", ";
              }
              if (log->modified_fields_[i]) {
                auto val = log->tuple_.GetValue(&partial_schema, new_idx++);
                if (val.IsNull()) {
                  log_string += "<NULL>";
                } else {
                  log_string += val.ToString();
                }
              } else {
                log_string += "_";
              }
            }
            log_string += ")";
          }
          fmt::println("\ttxn{}@{} {} ts={}", link.prev_txn_ ^ TXN_START_ID, link.prev_log_idx_, log_string, log->ts_);
          link = log->prev_version_;
        } else {
          break;
        }
      }
    }
    ++iter;
  }
}

}  // namespace bustub
