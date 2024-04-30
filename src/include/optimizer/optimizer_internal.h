#pragma once

#include "execution/expressions/abstract_expression.h"

namespace bustub {

// Note: You can define your optimizer helper functions here
auto IsPredicateConstant(const AbstractExpressionRef &expr) -> bool;

}  // namespace bustub
