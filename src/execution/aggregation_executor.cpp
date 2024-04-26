//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

void SimpleAggregationHashTable::CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
  for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
    switch (agg_types_[i]) {
      case AggregationType::CountStarAggregate:
        result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
        break;
      case AggregationType::CountAggregate:
        if (!input.aggregates_[i].IsNull()) {
          if (!result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          } else {
            result->aggregates_[i] = ValueFactory::GetIntegerValue(1);
          }
        }
        break;
      case AggregationType::SumAggregate:
        if (!input.aggregates_[i].IsNull()) {
          if (!result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          } else {
            result->aggregates_[i] = input.aggregates_[i];
          }
        }
        break;
      case AggregationType::MinAggregate:
        if (!input.aggregates_[i].IsNull()) {
          if (!result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
          } else {
            result->aggregates_[i] = input.aggregates_[i];
          }
        }
        break;
      case AggregationType::MaxAggregate:
        if (!input.aggregates_[i].IsNull()) {
          if (!result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
          } else {
            result->aggregates_[i] = input.aggregates_[i];
          }
        }
        break;
    }
  }
}

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_({}),
      schema_(plan->OutputSchema()) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  if (plan_->GetGroupBys().empty()) {
    aht_.MakeEmptyEntry();
  }
  if (child_executor_) {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      auto agg_key = MakeAggregateKey(&tuple);
      auto agg_val = MakeAggregateValue(&tuple);
      aht_.InsertCombine(agg_key, agg_val);
    }
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const auto &key = aht_iterator_.Key().group_bys_;
  const auto &val = aht_iterator_.Val().aggregates_;
  std::vector<Value> out_values;
  out_values.reserve(key.size() + val.size());
  out_values.insert(out_values.end(), key.begin(), key.end());
  out_values.insert(out_values.end(), val.begin(), val.end());
  *tuple = {out_values, &schema_};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
