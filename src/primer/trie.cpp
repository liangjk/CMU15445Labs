#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto now = root_;
  if (now == nullptr) {
    return nullptr;
  }
  for (char c : key) {
    auto it = now->children_.find(c);
    if (it == now->children_.end()) {
      return nullptr;
    }
    now = it->second;
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(now.get());
  if (node) {
    return node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> newroot;
  if (key.size() == 0) {
    if (root_ == nullptr) {
      newroot = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      newroot = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
    }
    return Trie(newroot);
  }
  if (root_ == nullptr) {
    newroot = std::make_shared<TrieNode>();
  } else {
    newroot = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  }
  auto node = newroot;
  for (auto strit = key.begin();; ++strit) {
    auto it = node->children_.find(*strit);
    if (strit + 1 != key.end()) {
      std::shared_ptr<TrieNode> newnode;
      if (it != node->children_.end()) {
        newnode = std::shared_ptr<TrieNode>(std::move(it->second->Clone()));
      } else {
        newnode = std::make_shared<TrieNode>();
      }
      node->children_[*strit] = newnode;
      node = newnode;
    } else {
      if (it != node->children_.end()) {
        it->second =
            std::make_shared<TrieNodeWithValue<T>>(it->second->children_, std::make_shared<T>(std::move(value)));
      } else {
        node->children_[*strit] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      }
      break;
    }
  }
  return Trie(newroot);
}

void Trie::RemoveNode(std::shared_ptr<TrieNode> &node, std::string_view::iterator strit,
                      const std::string_view::iterator &strend) {
  if (node == nullptr) {
    return;
  }
  if (strit == strend) {
    if (node->children_.size() == 0) {
      node = nullptr;
      return;
    } else {
      node = std::make_shared<TrieNode>(node->children_);
      return;
    }
  }
  auto it = node->children_.find(*strit);
  if (it == node->children_.end()) {
    return;
  }
  auto newnode = std::shared_ptr<TrieNode>(std::move(it->second->Clone()));
  RemoveNode(newnode, strit + 1, strend);
  if (newnode == nullptr) {
    if (!node->is_value_node_ && node->children_.size() <= 1) {
      node = nullptr;
    } else {
      node->children_.erase(it);
    }
  } else {
    it->second = newnode;
  }
  return;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  auto newroot = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  RemoveNode(newroot, key.begin(), key.end());
  return Trie(newroot);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
