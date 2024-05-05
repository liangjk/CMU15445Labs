//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()),
      out_schema_(plan->OutputSchema()),
      ht_iter_{ht_.cend(), ht_.cend()},
      join_type_(plan->GetJoinType()) {
  left_executor_->Init();
  Tuple tuple;
  RID rid;

  const auto &left_expressions = plan_->LeftJoinKeyExpressions();
  std::unordered_set<HashJoinKey> left_set;
  while (left_executor_->Next(&tuple, &rid)) {
    left_keys_.emplace_back(MakeHashJoinKey(&tuple, left_expressions, left_schema_));
    left_set.insert(left_keys_.back());
    left_values_.emplace_back(MakeHashJoinValue(&tuple, &left_schema_));
  }

  right_executor_->Init();
  const auto &right_expression = plan_->RightJoinKeyExpressions();
  while (right_executor_->Next(&tuple, &rid)) {
    auto right_key = MakeHashJoinKey(&tuple, right_expression, right_schema_);
    if (left_set.find(right_key) != left_set.end()) {
      ht_.insert({std::move(right_key), MakeHashJoinValue(&tuple, &right_schema_)});
    }
  }
  if (join_type_ == JoinType::LEFT) {
    auto &values = right_null_.values_;
    auto sz = right_schema_.GetColumnCount();
    values.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
      values.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
    }
  }
}

void HashJoinExecutor::Init() {
  left_cursor_ = 0;
  left_available_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
beginning:
  if (!left_available_) {
    if (left_cursor_ < left_keys_.size()) {
      left_available_ = true;
      right_available_ = false;
      ht_iter_ = ht_.equal_range(left_keys_[left_cursor_]);
    } else {
      return false;
    }
  }
  if (ht_iter_.first != ht_iter_.second) {
    std::vector<Value> out_values(left_values_[left_cursor_].values_);
    const auto &right_values = (ht_iter_.first++)->second.values_;
    out_values.reserve(out_values.size() + right_values.size());
    out_values.insert(out_values.end(), right_values.begin(), right_values.end());
    right_available_ = true;
    *tuple = {std::move(out_values), &out_schema_};
    return true;
  }
  left_available_ = false;
  if (!right_available_ && join_type_ == JoinType::LEFT) {
    std::vector<Value> out_values(left_values_[left_cursor_++].values_);
    const auto &right_values = right_null_.values_;
    out_values.reserve(out_values.size() + right_values.size());
    out_values.insert(out_values.end(), right_values.begin(), right_values.end());
    *tuple = {std::move(out_values), &out_schema_};
    return true;
  }
  ++left_cursor_;
  goto beginning;
}

}  // namespace bustub
