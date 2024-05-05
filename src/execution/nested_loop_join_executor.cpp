//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "optimizer/optimizer_internal.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()),
      out_schema_(plan->OutputSchema()),
      join_type_(plan->GetJoinType()) {
  const auto &filter = plan_->predicate_;
  if (IsPredicateConstant(filter)) {
    auto value = filter->Evaluate(nullptr, Schema({}));
    if (!value.IsNull() && !value.GetAs<bool>()) {
      predicate_false_ = true;
      return;
    }
  }
  left_values_.reserve(left_schema_.GetColumnCount());
}

void NestedLoopJoinExecutor::Init() {
  if (!predicate_false_) {
    left_executor_->Init();
    left_available_ = false;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (predicate_false_) {
    return false;
  }
beginning:
  RID child_rid;
  if (!left_available_) {
    if (left_executor_->Next(&left_tuple_, &child_rid)) {
      auto sz = left_schema_.GetColumnCount();
      left_values_.clear();
      for (size_t i = 0; i < sz; ++i) {
        left_values_.push_back(left_tuple_.GetValue(&left_schema_, i));
      }
      left_available_ = true;
      right_available_ = false;
      right_executor_->Init();
    } else {
      return false;
    }
  }
  Tuple right_tuple;
  while (right_executor_->Next(&right_tuple, &child_rid)) {
    auto value = plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_);
    if (!value.IsNull() && value.GetAs<bool>()) {
      right_available_ = true;
      std::vector<Value> out_values(left_values_);
      auto sz = right_schema_.GetColumnCount();
      out_values.reserve(left_values_.size() + sz);
      for (size_t i = 0; i < sz; ++i) {
        out_values.emplace_back(right_tuple.GetValue(&right_schema_, i));
      }
      *tuple = {std::move(out_values), &out_schema_};
      return true;
    }
  }
  left_available_ = false;
  if (!right_available_ && join_type_ == JoinType::LEFT) {
    std::vector<Value> out_values(left_values_);
    auto sz = right_schema_.GetColumnCount();
    out_values.reserve(left_values_.size() + sz);
    for (size_t i = 0; i < sz; ++i) {
      out_values.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
    }
    *tuple = {std::move(out_values), &out_schema_};
    return true;
  }
  goto beginning;
}

}  // namespace bustub
