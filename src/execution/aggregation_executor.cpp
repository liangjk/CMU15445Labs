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
#include "execution/expressions/column_value_expression.h"

namespace bustub {

void SimpleAggregationHashTable::CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
  for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
    if (dup_->at(i) != static_cast<size_t>(-1)) {
      continue;
    }
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
      aht_(plan->GetAggregates(), plan->GetAggregateTypes(), &dup_),
      aht_iterator_({}),
      dup_(plan_->GetAggregates().size(), static_cast<size_t>(-1)),
      schema_(plan->OutputSchema()) {
  if (plan_->GetGroupBys().empty()) {
    aht_.MakeEmptyEntry();
  }
  const auto &aggr_exprs = plan_->GetAggregates();
  const auto &aggr_types = plan_->GetAggregateTypes();
  ColumnValueExpression const *cols[aggr_exprs.size()];

  for (size_t i = 0; i < aggr_exprs.size(); ++i) {
    cols[i] = dynamic_cast<const ColumnValueExpression *>(aggr_exprs[i].get());
    if (cols[i] != nullptr) {
      auto colidx = cols[i]->GetColIdx();
      for (size_t from = 0; from < i; ++from) {
        if (cols[from] != nullptr && cols[from]->GetColIdx() == colidx && aggr_types[from] == aggr_types[i]) {
          dup_[i] = from;
          break;
        }
      }
    }
  }

  if (child_executor_) {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      auto agg_key = MakeAggregateKey(&tuple);
      auto agg_val = MakeAggregateValue(&tuple, aggr_exprs);
      aht_.InsertCombine(agg_key, agg_val);
    }
  }
}

void AggregationExecutor::Init() { aht_iterator_ = aht_.Begin(); }

auto AggregationExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const auto &key = aht_iterator_.Key().group_bys_;
  const auto &val = aht_iterator_.Val().aggregates_;
  std::vector<Value> out_values;
  out_values.reserve(key.size() + val.size());
  out_values.insert(out_values.end(), key.begin(), key.end());
  for (size_t i = 0; i < val.size(); ++i) {
    auto from = dup_[i];
    if (from != static_cast<size_t>(-1)) {
      out_values.emplace_back(val[from]);
    } else {
      out_values.emplace_back(val[i]);
    }
  }
  *tuple = {out_values, &schema_};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
