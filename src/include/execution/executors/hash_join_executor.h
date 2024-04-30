//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  std::vector<Value> keys_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  std::vector<Value> values_;
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hjkey) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hjkey.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return out_schema_; };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  const Schema &left_schema_;
  const Schema &right_schema_;
  const Schema &out_schema_;

  std::unordered_multimap<HashJoinKey, HashJoinValue> ht_{};
  std::pair<std::unordered_multimap<HashJoinKey, HashJoinValue>::const_iterator,
            std::unordered_multimap<HashJoinKey, HashJoinValue>::const_iterator>
      ht_iter_;

  JoinType join_type_;
  HashJoinValue right_null_{};

  std::vector<HashJoinKey> left_keys_;
  std::vector<HashJoinValue> left_values_;
  size_t left_cursor_{0};

  bool left_available_{false};
  bool right_available_{false};

  auto MakeHashJoinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &expressions, const Schema &schema)
      -> HashJoinKey {
    std::vector<Value> keys;
    auto sz = expressions.size();
    keys.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
      keys.emplace_back(expressions[i]->Evaluate(tuple, schema));
    }
    return {keys};
  }

  auto MakeHashJoinValue(const Tuple *tuple, const Schema *schema) -> HashJoinValue {
    std::vector<Value> values;
    auto sz = schema->GetColumnCount();
    values.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
      values.push_back(tuple->GetValue(schema, i));
    }
    return {values};
  }
};

}  // namespace bustub
