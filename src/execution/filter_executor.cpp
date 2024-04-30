#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "optimizer/optimizer_internal.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  const auto &filter = plan_->GetPredicate();
  if (IsPredicateConstant(filter)) {
    auto value = filter->Evaluate(nullptr, Schema({}));
    if (!value.IsNull() && !value.GetAs<bool>()) {
      predicate_false_ = true;
    }
  }
}

void FilterExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (predicate_false_) {
    return false;
  }
  auto filter_expr = plan_->GetPredicate();

  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);

    if (!status) {
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

}  // namespace bustub
