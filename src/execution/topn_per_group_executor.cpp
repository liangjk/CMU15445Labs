//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_per_group_executor.h"
#include "type/value_factory.h"

namespace bustub {

TopNPerGroupExecutor::TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      child_schema_(child_executor_->GetOutputSchema()) {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  const auto &partition = plan_->GetGroupBy();
  const auto &order_bys = plan->GetOrderBy();
  auto n = plan_->GetN();
  if (n == 0) {
    return;
  }

  using Heap = std::multiset<TupleToSort, TupleSorter>;

  std::unordered_map<AggregateKey, Heap> ht;
  TupleSorter sorter(order_bys);
  while (child_executor_->Next(&tuple, &rid)) {
    auto group_key = MakeAggregateKey(&tuple, partition);
    std::vector<Value> order_key;
    order_key.reserve(order_bys.size());
    for (const auto &pair : order_bys) {
      order_key.emplace_back(pair.second->Evaluate(&tuple, child_schema_));
    }
    auto htiter = ht.find(group_key);
    if (htiter == ht.end()) {
      Heap heap{sorter};
      heap.emplace(tuple, rid, order_key);
      ht.insert({std::move(group_key), std::move(heap)});
    } else {
      auto &heap = htiter->second;
      if (heap.size() < n) {
        heap.emplace(tuple, rid, order_key);
      } else {
        TupleToSort toinsert(tuple, rid, order_key);
        if (!sorter(*heap.rbegin(), toinsert)) {
          heap.emplace(std::move(toinsert));
          auto &top = *heap.rbegin();
          if (heap.size() - heap.count(top) >= n) {
            heap.erase(heap.lower_bound(top), heap.end());
          }
        }
      }
    }
  }

  const auto &out_schema = plan_->OutputSchema();
  const auto &exprs = plan_->expressions_;

  for (const auto &pair : ht) {
    int rank = 0;
    int current = 0;
    auto &heap = pair.second;
    TupleToSort const *last = nullptr;
    for (const auto &origin : heap) {
      ++current;
      if (last == nullptr || !sorter.Equal(origin, *last)) {
        rank = current;
        last = &origin;
      }
      std::vector<Value> out_values;
      out_values.reserve(exprs.size());
      for (const auto &expr : exprs) {
        auto col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
        if (col_expr->GetColIdx() != static_cast<uint32_t>(-1)) {
          out_values.emplace_back(expr->Evaluate(&origin.tuple_, out_schema));
        } else {
          out_values.emplace_back(ValueFactory::GetIntegerValue(rank));
        }
      }
      data_.emplace_back(std::move(out_values), &out_schema);
    }
  }
}

void TopNPerGroupExecutor::Init() { iter_ = data_.begin(); }

auto TopNPerGroupExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == data_.end()) {
    return false;
  }
  *tuple = *iter_++;
  return true;
}

}  // namespace bustub
