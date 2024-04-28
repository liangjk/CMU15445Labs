#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (child_executor) {
    SetChildExecutor(std::move(child_executor));
  }
}

void TopNExecutor::SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
  child_executor_ = std::move(child_executor);
  const auto &order_bys = plan_->GetOrderBy();
  auto exprs = order_bys.size();
  std::priority_queue<TupleToSort, std::vector<TupleToSort>, TupleSorter> heap{TupleSorter(order_bys)};
  auto n = plan_->GetN();

  child_executor_->Init();
  const auto &schema = child_executor_->GetOutputSchema();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> sort;
    sort.reserve(exprs);
    for (size_t i = 0; i < exprs; ++i) {
      sort.push_back(order_bys[i].second->Evaluate(&tuple, schema));
    }
    heap.emplace(tuple, rid, sort);
    if (heap_cnt_ < n) {
      ++heap_cnt_;
    } else {
      heap.pop();
    }
  }

  data_.reserve(heap_cnt_);
  for (size_t i = 0; i < heap_cnt_; ++i) {
    data_.emplace_back(std::move(const_cast<TupleToSort &>(heap.top())));
    heap.pop();
  }
}

void TopNExecutor::Init() { iter_ = data_.crbegin(); }

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == data_.crend()) {
    return false;
  }
  *tuple = iter_->tuple_;
  *rid = iter_->rid_;
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_cnt_; };

}  // namespace bustub
