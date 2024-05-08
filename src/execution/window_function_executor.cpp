#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_schema_(child_executor->GetOutputSchema()) {
  child_executor->Init();
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys =
      plan_->window_functions_.begin()->second.order_by_;
  auto exprs = order_bys.size();

  Tuple tuple;
  RID rid;
  while (child_executor->Next(&tuple, &rid)) {
    data_.emplace_back(tuple, rid);
  }

  if (exprs > 0) {
    sort_ = true;
    for (auto &data : data_) {
      data.values_.reserve(exprs);
      for (size_t i = 0; i < exprs; ++i) {
        data.values_.push_back(order_bys[i].second->Evaluate(&data.tuple_, child_schema_));
      }
    }
    std::sort(data_.begin(), data_.end(), TupleSorter(order_bys));
    return;
  }

  for (auto &data : data_) {
    for (const auto &pair : plan_->window_functions_) {
      CalcAndInsert(data, hts_[pair.first], pair.second);
    }
  }

  for (auto &data : data_) {
    std::vector<Value> new_values;
    auto sz = plan_->columns_.size();
    new_values.resize(sz);
    for (uint32_t i = 0; i < sz; ++i) {
      const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*plan_->columns_[i]);
      auto idx = col_val.GetColIdx();
      if (idx != static_cast<uint32_t>(-1)) {
        new_values[i] = data.tuple_.GetValue(&child_schema_, idx);
      } else {
        new_values[i] = hts_[i].at(MakeAggregateKey(&data.tuple_, plan_->window_functions_.at(i).partition_by_));
      }
    }
    data.tuple_ = {std::move(new_values), plan_->output_schema_.get()};
  }
}

void WindowFunctionExecutor::Init() {
  iter_ = data_.cbegin();
  if (sort_) {
    hts_.clear();
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (iter_ == data_.cend()) {
    return false;
  }
  if (sort_) {
    std::vector<Value> out_values;
    auto sz = plan_->columns_.size();
    out_values.resize(sz);
    for (uint32_t i = 0; i < sz; ++i) {
      const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*plan_->columns_[i]);
      auto idx = col_val.GetColIdx();
      if (idx != static_cast<uint32_t>(-1)) {
        out_values[i] = iter_->tuple_.GetValue(&child_schema_, idx);
      } else {
        out_values[i] = CalcAndInsert(*iter_, hts_[i], plan_->window_functions_.at(i));
      }
    }
    *tuple = {std::move(out_values), plan_->output_schema_.get()};
  } else {
    *tuple = iter_->tuple_;
  }
  ++iter_;
  return true;
}

auto WindowFunctionExecutor::CalcAndInsert(const TupleToSort &tuple, std::unordered_map<AggregateKey, Value> &ht,
                                           const WindowFunctionPlanNode::WindowFunction &func) -> Value {
  auto key = MakeAggregateKey(&tuple.tuple_, func.partition_by_);
  auto val = func.function_->Evaluate(&tuple.tuple_, child_schema_);
  Value ret;
  if (ht.find(key) == ht.end()) {
    switch (func.type_) {
      case WindowFunctionType::CountStarAggregate:
        ret = ValueFactory::GetIntegerValue(1);
        ht[key] = ret;
        return ret;
      case WindowFunctionType::CountAggregate:
        if (val.IsNull()) {
          ret = ValueFactory ::GetIntegerValue(0);
        } else {
          ret = ValueFactory::GetIntegerValue(1);
        }
        ht[key] = ret;
        return ret;
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::MinAggregate:
        ht[key] = val;
        return val;
      case WindowFunctionType::Rank:
        RankValue rv{1, 1, &tuple.values_};
        ht[key] = ValueFactory::GetVarcharValue(reinterpret_cast<char *>(&rv), sizeof(RankValue), true);
        return ValueFactory::GetIntegerValue(1);
    }
  }
  switch (func.type_) {
    case WindowFunctionType::CountStarAggregate:
      ret = ht[key].Add(ValueFactory::GetIntegerValue(1));
      ht[key] = ret;
      return ret;
    case WindowFunctionType::CountAggregate:
      if (val.IsNull()) {
        return ht[key];
      }
      ret = ht[key].Add(ValueFactory::GetIntegerValue(1));
      ht[key] = ret;
      return ret;
    case WindowFunctionType::SumAggregate:
      ret = ht[key].Add(val);
      ht[key] = ret;
      return ret;
    case WindowFunctionType::MaxAggregate:
      ret = ht[key].Max(val);
      ht[key] = ret;
      return ret;
    case WindowFunctionType::MinAggregate:
      ret = ht[key].Min(val);
      ht[key] = ret;
      return ret;
    case WindowFunctionType::Rank:
      auto rv = reinterpret_cast<RankValue *>(const_cast<char *>(ht[key].GetData()));
      ++rv->count_;
      bool equal = true;
      for (size_t i = 0; i < tuple.values_.size(); ++i) {
        if (rv->last_order_->at(i).CompareEquals(tuple.values_[i]) != CmpBool::CmpTrue) {
          equal = false;
          break;
        }
      }
      if (!equal) {
        rv->current_ = rv->count_;
        rv->last_order_ = &tuple.values_;
      }
      return ValueFactory::GetIntegerValue(rv->current_);
  }
  BUSTUB_ASSERT(false, "should not arrive here");
}
}  // namespace bustub
