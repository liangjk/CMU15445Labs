#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

static auto GetColIdxAndValues(uint32_t &col_idx, const AbstractExpressionRef &expr)
    -> std::unique_ptr<std::vector<Value>> {
  if (expr == nullptr) {
    return nullptr;
  }
  if (expr->GetReturnType().GetType() != TypeId::BOOLEAN) {
    return nullptr;
  }
  if (auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ != ComparisonType::Equal) {
      return nullptr;
    }
    const auto &lhs = comp_expr->GetChildAt(0);
    const auto &rhs = comp_expr->GetChildAt(1);
    if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(lhs.get())) {
      if (auto const_expr = dynamic_cast<const ConstantValueExpression *>(rhs.get())) {
        col_idx = col_expr->GetColIdx();
        return std::make_unique<std::vector<Value>>(1, const_expr->val_);
      }
    } else if (auto const_expr = dynamic_cast<const ConstantValueExpression *>(lhs.get())) {
      if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(rhs.get())) {
        col_idx = col_expr->GetColIdx();
        return std::make_unique<std::vector<Value>>(1, const_expr->val_);
      }
    }
    return nullptr;
  }
  if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    const auto &lhs = logic_expr->GetChildAt(0);
    const auto &rhs = logic_expr->GetChildAt(1);
    uint32_t lcol;
    uint32_t rcol;
    auto lval = GetColIdxAndValues(lcol, lhs);
    auto rval = GetColIdxAndValues(rcol, rhs);
    if (lval == nullptr || rval == nullptr || lcol != rcol) {
      return nullptr;
    }
    col_idx = lcol;
    auto ret = std::make_unique<std::vector<Value>>();
    auto lsize = lval->size();
    auto rsize = rval->size();
    if (logic_expr->logic_type_ == LogicType::And) {
      ret->reserve(lsize <= rsize ? lsize : rsize);
      for (size_t lidx = 0; lidx < lsize; ++lidx) {
        const auto &cur_val = lval->at(lidx);
        for (size_t ridx = 0; ridx < rsize; ++ridx) {
          if (cur_val.CompareEquals(rval->at(ridx)) == CmpBool::CmpTrue) {
            ret->push_back(cur_val);
            break;
          }
        }
      }
    } else {
      ret->reserve(lsize + rsize);
      for (size_t lidx = 0; lidx < lsize; ++lidx) {
        const auto &cur_val = lval->at(lidx);
        bool found = false;
        for (size_t ridx = 0; ridx < rsize; ++ridx) {
          if (cur_val.CompareEquals(rval->at(ridx)) == CmpBool::CmpTrue) {
            found = true;
            break;
          }
        }
        if (!found) {
          ret->push_back(cur_val);
        }
      }
      for (size_t ridx = 0; ridx < rsize; ++ridx) {
        ret->push_back(rval->at(ridx));
      }
    }
    return ret;
  }
  return nullptr;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    uint32_t col_idx;
    auto values = GetColIdxAndValues(col_idx, seq_plan.filter_predicate_);
    if (values) {
      auto indexes = catalog_.GetTableIndexes(seq_plan.table_name_);
      for (const auto &index : indexes) {
        auto key_attr = index->index_->GetKeyAttrs();
        if (key_attr.size() == 1 && key_attr[0] == col_idx) {
          std::vector<AbstractExpressionRef> pred_keys;
          auto vsize = values->size();
          pred_keys.reserve(vsize);
          for (size_t i = 0; i < vsize; ++i) {
            pred_keys.push_back(std::make_shared<ConstantValueExpression>(values->at(i)));
          }
          return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.table_oid_, index->index_oid_,
                                                     seq_plan.filter_predicate_, pred_keys);
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
