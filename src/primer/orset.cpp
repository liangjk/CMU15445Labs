#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
  for (const auto &pair : live)
    if (pair.element == elem) return true;
  return false;
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
  auto pair = elePair(elem, uid);
  live.insert(pair);
  tomb.erase(pair);
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Remove is not implemented");
  for (auto it = live.begin(); it != live.end();)
    if (it->element == elem) {
      tomb.insert(*it);
      it = live.erase(it);
    } else
      ++it;
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Merge is not implemented");
  for (const auto &pair : other.tomb) {
    live.erase(pair);
    tomb.insert(pair);
  }
  for (const auto &pair : other.live)
    if (tomb.find(pair) == tomb.end()) live.insert(pair);
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Elements is not implemented");
  auto elements = std::unordered_set<T>();
  for (const auto &pair : live) elements.insert(pair.element);
  return std::vector<T>(elements.begin(), elements.end());
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
