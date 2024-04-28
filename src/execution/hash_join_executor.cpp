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
      left_expressions_(plan->LeftJoinKeyExpressions()),
      ht_iter_{ht_.cend(), ht_.cend()},
      join_type_(plan->GetJoinType()) {
  right_executor_->Init();
  const auto &right_expression = plan_->RightJoinKeyExpressions();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    ht_.insert({MakeHashJoinKey(&right_tuple, right_expression, right_schema_),
                MakeHashJoinValue(&right_tuple, &right_schema_)});
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
  left_executor_->Init();
  left_available_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
beginning:
  if (!left_available_) {
    Tuple left_tuple;
    RID left_rid;
    if (left_executor_->Next(&left_tuple, &left_rid)) {
      left_available_ = true;
      right_available_ = false;
      left_values_ = MakeHashJoinValue(&left_tuple, &left_schema_);
      ht_iter_ = ht_.equal_range(MakeHashJoinKey(&left_tuple, left_expressions_, left_schema_));
    } else {
      return false;
    }
  }
  if (ht_iter_.first != ht_iter_.second) {
    std::vector<Value> out_values(left_values_.values_);
    const auto &right_values = (ht_iter_.first++)->second.values_;
    out_values.reserve(out_values.size() + right_values.size());
    out_values.insert(out_values.end(), right_values.begin(), right_values.end());
    right_available_ = true;
    *tuple = {out_values, &out_schema_};
    return true;
  }
  left_available_ = false;
  if (!right_available_ && join_type_ == JoinType::LEFT) {
    std::vector<Value> out_values(left_values_.values_);
    const auto &right_values = right_null_.values_;
    out_values.reserve(out_values.size() + right_values.size());
    out_values.insert(out_values.end(), right_values.begin(), right_values.end());
    *tuple = {out_values, &out_schema_};
    return true;
  }
  goto beginning;
}

}  // namespace bustub
