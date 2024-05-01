//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.h
//
// Identification: src/include/execution/executors/topn_per_group_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/topn_per_group_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNPerGroupExecutor executor executes a topn.
 */
class TopNPerGroupExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNPerGroupExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopNPerGroup plan to be executed
   */
  TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                       std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopNPerGroup */
  void Init() override;

  /**
   * Yield the next tuple from the TopNPerGroup.
   * @param[out] tuple The next tuple produced by the TopNPerGroup
   * @param[out] rid The next tuple RID produced by the TopNPerGroup
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The TopNPerGroup plan node to be executed */
  const TopNPerGroupPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  const Schema &child_schema_;

  std::vector<Tuple> data_;
  std::vector<Tuple>::iterator iter_;

  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &exprs) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(exprs.size());
    for (const auto &expr : exprs) {
      keys.emplace_back(expr->Evaluate(tuple, child_schema_));
    }
    return {keys};
  }
};
}  // namespace bustub
