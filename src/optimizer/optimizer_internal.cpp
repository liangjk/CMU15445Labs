#include <algorithm>

#include "execution/expressions/column_value_expression.h"
#include "optimizer/optimizer_internal.h"
namespace bustub {

auto IsPredicateConstant(const AbstractExpressionRef &expr) -> bool {
  if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
    return false;
  }
  return std::all_of(expr->children_.begin(), expr->children_.end(), IsPredicateConstant);
}

}  // namespace bustub
