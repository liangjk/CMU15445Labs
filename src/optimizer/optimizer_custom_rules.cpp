#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/topn_per_group_plan.h"
#include "execution/plans/window_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeColumnPruning(p);
  p = OptimizeProjectOnAggregation(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeWindowFuncToTopNGroup(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  return p;
}

static auto GetColLimitPosInt(const AbstractExpressionRef &pred, uint32_t &col, int &value) -> bool {
  if (auto comp_expr = dynamic_cast<const ComparisonExpression *>(pred.get())) {
    const auto &lhs = comp_expr->children_[0];
    const auto &rhs = comp_expr->children_[1];
    if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(lhs.get())) {
      if (auto const_exp = dynamic_cast<const ConstantValueExpression *>(rhs.get())) {
        auto const_val = const_exp->val_;
        if (const_val.CheckInteger()) {
          if (comp_expr->comp_type_ == ComparisonType::LessThan) {
            col = col_expr->GetColIdx();
            value = const_val.GetAs<int>() - 1;
            return value >= 0;
          }
          if (comp_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
            col = col_expr->GetColIdx();
            value = const_val.GetAs<int>();
            return value >= 0;
          }
        }
      }
    } else if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(rhs.get())) {
      if (auto const_exp = dynamic_cast<const ConstantValueExpression *>(lhs.get())) {
        auto const_val = const_exp->val_;
        if (const_val.CheckInteger()) {
          if (comp_expr->comp_type_ == ComparisonType::GreaterThan) {
            col = col_expr->GetColIdx();
            value = const_val.GetAs<int>() - 1;
            return value >= 0;
          }
          if (comp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
            col = col_expr->GetColIdx();
            value = const_val.GetAs<int>();
            return value >= 0;
          }
        }
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeWindowFuncToTopNGroup(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeWindowFuncToTopNGroup(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    int value;
    uint32_t col;
    if (GetColLimitPosInt(filter_plan.predicate_, col, value)) {
      const auto &child_plan = *optimized_plan->children_[0];
      if (child_plan.GetType() == PlanType::Window) {
        const auto &window_plan = dynamic_cast<const WindowFunctionPlanNode &>(child_plan);
        const auto &window_funcs = window_plan.window_functions_;
        if (window_funcs.size() == 1 && window_funcs.find(col) != window_funcs.end()) {
          const auto &col_func = window_funcs.at(col);
          if (col_func.type_ == WindowFunctionType::Rank) {
            return std::make_shared<TopNPerGroupPlanNode>(window_plan.output_schema_, window_plan.GetChildAt(0),
                                                          window_plan.columns_, col_func.partition_by_,
                                                          col_func.order_by_, value);
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
