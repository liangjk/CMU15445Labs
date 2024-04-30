#include <unordered_set>

#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

static auto PlugColumn(const AbstractExpressionRef &expr, const std::vector<AbstractExpressionRef> &columns)
    -> AbstractExpressionRef {
  if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
    return columns[col_expr->GetColIdx()];
  }
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(PlugColumn(child, columns));
  }
  return expr->CloneWithChildren(std::move(children));
}

static void SetRelevant(const AbstractExpressionRef &expr, std::unordered_set<uint32_t> &relevant, size_t key_size) {
  if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
    auto col_idx = col_expr->GetColIdx();
    if (col_idx >= key_size) {
      relevant.insert(col_idx);
    }
    return;
  }
  for (const auto &child : expr->GetChildren()) {
    SetRelevant(child, relevant, key_size);
  }
}

/**
 * @note You may use this function to implement column pruning optimization.
 */
auto Optimizer::OptimizeColumnPruning(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Your code here
  if (plan->GetType() == PlanType::Projection) {
    const auto &parent_proj = dynamic_cast<const ProjectionPlanNode &>(*plan);
    const auto &child_plan = plan->children_[0];
    if (child_plan->GetType() == PlanType::Projection) {
      const auto &child_proj = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
      std::vector<AbstractExpressionRef> new_exprs;
      const auto &parent_exprs = parent_proj.GetExpressions();
      const auto &child_exprs = child_proj.GetExpressions();
      new_exprs.reserve(parent_exprs.size());
      for (const auto &expr : parent_exprs) {
        new_exprs.emplace_back(PlugColumn(expr, child_exprs));
      }
      auto optimized_plan = std::make_shared<ProjectionPlanNode>(parent_proj.output_schema_, std::move(new_exprs),
                                                                 child_proj.GetChildAt(0));
      return OptimizeColumnPruning(optimized_plan);
    }
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  return plan->CloneWithChildren(std::move(children));
}

auto Optimizer::OptimizeProjectOnAggregation(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeProjectOnAggregation(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() == PlanType::Aggregation) {
      const auto &aggre_plan = dynamic_cast<const AggregationPlanNode &>(*child_plan);
      std::unordered_set<uint32_t> relevant;
      const auto &old_projexprs = projection_plan.GetExpressions();
      const auto &old_schema = aggre_plan.OutputSchema();
      auto oldsz = old_schema.GetColumnCount();
      auto gbsz = aggre_plan.group_bys_.size();
      for (const auto &expr : old_projexprs) {
        SetRelevant(expr, relevant, gbsz);
      }
      auto sz = relevant.size();
      if (gbsz + sz < oldsz) {
        std::vector<AbstractExpressionRef> new_proj(oldsz, nullptr);
        size_t new_idx = 0;
        std::vector<Column> new_columns;
        new_columns.reserve(gbsz + sz);
        for (size_t i = 0; i < gbsz; ++i) {
          auto &col = old_schema.GetColumn(i);
          new_columns.emplace_back(col);
          new_proj[i] = std::make_shared<ColumnValueExpression>(0, new_idx++, Column{col});
        }

        std::vector<AbstractExpressionRef> new_aggexprs;
        new_aggexprs.reserve(sz);
        std::vector<AggregationType> new_aggtypes;
        new_aggtypes.reserve(sz);

        for (auto old_index : relevant) {
          new_aggexprs.emplace_back(aggre_plan.aggregates_[old_index - gbsz]);
          new_aggtypes.emplace_back(aggre_plan.agg_types_[old_index - gbsz]);
          auto &col = old_schema.GetColumn(old_index);
          new_columns.emplace_back(col);
          new_proj[old_index] = std::make_shared<ColumnValueExpression>(0, new_idx++, Column{col});
        }

        auto new_child = std::make_shared<AggregationPlanNode>(
            std::make_shared<Schema>(std::move(new_columns)), aggre_plan.GetChildAt(0),
            std::vector<AbstractExpressionRef>{aggre_plan.group_bys_}, std::move(new_aggexprs),
            std::move(new_aggtypes));
        std::vector<AbstractExpressionRef> new_projexprs;
        new_projexprs.reserve(old_projexprs.size());
        for (const auto &expr : old_projexprs) {
          new_projexprs.emplace_back(PlugColumn(expr, new_proj));
        }
        return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, std::move(new_projexprs),
                                                    std::move(new_child));
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
