#include "execution/executors/sort_executor.h"

namespace bustub {

auto TupleSorter::operator()(const TupleToSort &a, const TupleToSort &b) const -> bool {
  auto sz = order_bys_.size();
  for (size_t i = 0; i < sz; ++i) {
    bool desc = order_bys_[i].first == OrderByType::DESC;
    if (a.values_[i].CompareLessThan(b.values_[i]) == CmpBool::CmpTrue) {
      return !desc;
    }
    if (a.values_[i].CompareGreaterThan(b.values_[i]) == CmpBool::CmpTrue) {
      return desc;
    }
  }
  return false;
}

auto TupleSorter::Equal(const TupleToSort &a, const TupleToSort &b) const -> bool {
  auto sz = order_bys_.size();
  for (size_t i = 0; i < sz; ++i) {
    if (a.values_[i].CompareEquals(b.values_[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor->Init();

  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys = plan_->GetOrderBy();
  auto exprs = order_bys.size();
  const auto &schema = child_executor->GetOutputSchema();

  Tuple tuple;
  RID rid;
  while (child_executor->Next(&tuple, &rid)) {
    data_.emplace_back(tuple, rid);
    auto &back = data_.back();
    back.values_.reserve(exprs);
    for (size_t i = 0; i < exprs; ++i) {
      back.values_.push_back(order_bys[i].second->Evaluate(&back.tuple_, schema));
    }
  }

  std::sort(data_.begin(), data_.end(), TupleSorter(order_bys));
}

void SortExecutor::Init() { iter_ = data_.cbegin(); }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == data_.cend()) {
    return false;
  }
  *tuple = iter_->tuple_;
  *rid = iter_->rid_;
  ++iter_;
  return true;
}

}  // namespace bustub
