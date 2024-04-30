#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

static auto SplitPred(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &hash_left,
                      std::vector<AbstractExpressionRef> &hash_right, std::vector<AbstractExpressionRef> &left_pred,
                      std::vector<AbstractExpressionRef> &right_pred) -> bool {
  // if (expr->GetReturnType().GetType() != TypeId::BOOLEAN) {
  //   return false;
  // }
  if (auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    const auto &lhs = comp_expr->GetChildAt(0);
    const auto &rhs = comp_expr->GetChildAt(1);
    int left_related = -1;
    int right_related = -1;
    const ColumnValueExpression *right_col_expr[2] = {nullptr};
    if (auto left_col = dynamic_cast<const ColumnValueExpression *>(lhs.get())) {
      if (left_col->GetTupleIdx() == 0) {
        left_related = 0;
      } else {
        right_related = 0;
        right_col_expr[0] = left_col;
      }
    } else if (auto left_const = dynamic_cast<const ConstantValueExpression *>(lhs.get()); left_const == nullptr) {
      return false;
    }
    if (auto right_col = dynamic_cast<const ColumnValueExpression *>(rhs.get())) {
      if (right_col->GetTupleIdx() == 0) {
        left_related = 1;
      } else {
        right_related = 1;
        right_col_expr[1] = right_col;
      }
    } else if (auto right_const = dynamic_cast<const ConstantValueExpression *>(rhs.get()); right_const == nullptr) {
      return false;
    }
    if (left_related >= 0 && right_related >= 0) {
      if (comp_expr->comp_type_ == ComparisonType::Equal) {
        if (left_related == 0) {
          hash_left.emplace_back(lhs);
          hash_right.emplace_back(rhs);
        } else {
          hash_left.emplace_back(rhs);
          hash_right.emplace_back(lhs);
        }
        return true;
      }
      return false;
    }
    if (right_related >= 0) {
      AbstractExpressionRef left_comp;
      AbstractExpressionRef right_comp;
      if (right_col_expr[0] == nullptr) {
        left_comp = lhs;
      } else {
        left_comp = std::make_shared<ColumnValueExpression>(0, right_col_expr[0]->GetColIdx(),
                                                            right_col_expr[0]->GetReturnType());
      }
      if (right_col_expr[1] == nullptr) {
        right_comp = rhs;
      } else {
        right_comp = std::make_shared<ColumnValueExpression>(0, right_col_expr[1]->GetColIdx(),
                                                             right_col_expr[1]->GetReturnType());
      }
      right_pred.emplace_back(
          std::make_shared<ComparisonExpression>(std::move(left_comp), std::move(right_comp), comp_expr->comp_type_));
      return true;
    }
    left_pred.emplace_back(expr);
    return true;
  }
  if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    const auto &rhs = logic_expr->GetChildAt(1);
    if (!SplitPred(rhs, hash_left, hash_right, left_pred, right_pred)) {
      return false;
    }
    const auto &lhs = logic_expr->GetChildAt(0);
    return SplitPred(lhs, hash_left, hash_right, left_pred, right_pred);
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    const auto &old_pred = nlj_plan.Predicate();
    if (old_pred) {
      std::vector<AbstractExpressionRef> hash_left;
      std::vector<AbstractExpressionRef> hash_right;
      std::vector<AbstractExpressionRef> left_pred;
      std::vector<AbstractExpressionRef> right_pred;
      if (SplitPred(old_pred, hash_left, hash_right, left_pred, right_pred)) {
        AbstractPlanNodeRef left_child = nlj_plan.GetChildAt(0);
        auto left_nlj = dynamic_cast<const NestedLoopJoinPlanNode *>(left_child.get());
        AbstractPlanNodeRef right_child = nlj_plan.GetChildAt(1);
        auto right_nlj = dynamic_cast<const NestedLoopJoinPlanNode *>(right_child.get());

        auto lsz = left_pred.size();
        switch (lsz) {
          case 0:
            break;
          case 1:
            if (left_nlj != nullptr) {
              const auto &left_plan = left_nlj->GetLeftPlan();
              const auto &right_plan = left_nlj->GetRightPlan();
              auto new_pred = RewriteExpressionForJoin(left_pred[0], left_plan->OutputSchema().GetColumnCount(),
                                                       right_plan->OutputSchema().GetColumnCount());
              const auto &old_pred = left_nlj->Predicate();
              if (!IsPredicateTrue(old_pred)) {
                new_pred = std::make_shared<LogicExpression>(std::move(new_pred), old_pred, LogicType::And);
              }
              left_child = std::make_shared<NestedLoopJoinPlanNode>(left_nlj->output_schema_, left_plan, right_plan,
                                                                    std::move(new_pred), left_nlj->GetJoinType());
            } else {
              left_child =
                  std::make_shared<FilterPlanNode>(left_child->output_schema_, std::move(left_pred[0]), left_child);
            }
            break;
          default:
            AbstractExpressionRef combine_pred =
                std::make_shared<LogicExpression>(std::move(left_pred[0]), std::move(left_pred[1]), LogicType::And);
            for (size_t i = 2; i < lsz; ++i) {
              combine_pred =
                  std::make_shared<LogicExpression>(std::move(combine_pred), std::move(left_pred[i]), LogicType::And);
            }
            if (left_nlj != nullptr) {
              const auto &left_plan = left_nlj->GetLeftPlan();
              const auto &right_plan = left_nlj->GetRightPlan();
              auto new_pred = RewriteExpressionForJoin(combine_pred, left_plan->OutputSchema().GetColumnCount(),
                                                       right_plan->OutputSchema().GetColumnCount());
              const auto &old_pred = left_nlj->Predicate();
              if (!IsPredicateTrue(old_pred)) {
                new_pred = std::make_shared<LogicExpression>(std::move(new_pred), old_pred, LogicType::And);
              }
              left_child = std::make_shared<NestedLoopJoinPlanNode>(left_nlj->output_schema_, left_plan, right_plan,
                                                                    std::move(new_pred), left_nlj->GetJoinType());
            } else {
              left_child =
                  std::make_shared<FilterPlanNode>(left_child->output_schema_, std::move(combine_pred), left_child);
            }
        }

        auto rsz = right_pred.size();
        switch (rsz) {
          case 0:
            break;
          case 1:
            if (right_nlj != nullptr) {
              const auto &left_plan = right_nlj->GetLeftPlan();
              const auto &right_plan = right_nlj->GetRightPlan();
              auto new_pred = RewriteExpressionForJoin(right_pred[0], left_plan->OutputSchema().GetColumnCount(),
                                                       right_plan->OutputSchema().GetColumnCount());
              const auto &old_pred = right_nlj->Predicate();
              if (!IsPredicateTrue(old_pred)) {
                new_pred = std::make_shared<LogicExpression>(std::move(new_pred), old_pred, LogicType::And);
              }
              right_child = std::make_shared<NestedLoopJoinPlanNode>(right_nlj->output_schema_, left_plan, right_plan,
                                                                     std::move(new_pred), right_nlj->GetJoinType());
            } else {
              right_child =
                  std::make_shared<FilterPlanNode>(right_child->output_schema_, std::move(right_pred[0]), right_child);
            }
            break;
          default:
            AbstractExpressionRef combine_pred =
                std::make_shared<LogicExpression>(std::move(right_pred[0]), std::move(right_pred[1]), LogicType::And);
            for (size_t i = 2; i < rsz; ++i) {
              combine_pred =
                  std::make_shared<LogicExpression>(std::move(combine_pred), std::move(right_pred[i]), LogicType::And);
            }
            if (right_nlj != nullptr) {
              const auto &left_plan = right_nlj->GetLeftPlan();
              const auto &right_plan = right_nlj->GetRightPlan();
              auto new_pred = RewriteExpressionForJoin(combine_pred, left_plan->OutputSchema().GetColumnCount(),
                                                       right_plan->OutputSchema().GetColumnCount());
              const auto &old_pred = right_nlj->Predicate();
              if (!IsPredicateTrue(old_pred)) {
                new_pred = std::make_shared<LogicExpression>(std::move(new_pred), old_pred, LogicType::And);
              }
              right_child = std::make_shared<NestedLoopJoinPlanNode>(right_nlj->output_schema_, left_plan, right_plan,
                                                                     std::move(new_pred), right_nlj->GetJoinType());
            } else {
              right_child =
                  std::make_shared<FilterPlanNode>(right_child->output_schema_, std::move(combine_pred), right_child);
            }
        }

        left_child = OptimizeNLJAsHashJoin(left_child);
        right_child = OptimizeNLJAsHashJoin(right_child);

        if (!hash_left.empty()) {
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, std::move(left_child),
                                                    std::move(right_child), std::move(hash_left), std::move(hash_right),
                                                    nlj_plan.join_type_);
        }
        return std::make_shared<NestedLoopJoinPlanNode>(
            nlj_plan.output_schema_, std::move(left_child), std::move(right_child),
            std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)), nlj_plan.join_type_);
      }
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  return plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
