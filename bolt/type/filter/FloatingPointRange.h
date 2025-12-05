/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/dynamic.h>
#include <type_traits>
#include "bolt/common/base/SimdUtil.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {

/// Range filter for floating point data types. Supports open, closed and
/// unbounded ranges, e.g. c >= 10.3, c > 10.3, c <= 34.8, c < 34.8, c >= 10.3
/// AND c < 34.8, c BETWEEN 10.3 and 34.8.
/// @tparam T Floating point type: float or double.
template <typename T>
class FloatingPointRange final : public AbstractRange {
  static_assert(
      std::is_same_v<T, float> || std::is_same_v<T, double>,
      "T must be float or double");

 public:
  FloatingPointRange(
      T lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      T upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed);

  FloatingPointRange(const FloatingPointRange& other, bool nullAllowed);

  // Core interface methods
  double lower() const {
    return lower_;
  }
  double upper() const {
    return upper_;
  }

  // Virtual interface implementation
  folly::dynamic serialize() const override;
  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;
  bool testDouble(double value) const final;
  bool testFloat(float value) const final;

  xsimd::batch_bool<double> testValues(xsimd::batch<double> values) const final;
  xsimd::batch_bool<float> testValues(xsimd::batch<float> values) const final;

  bool testDoubleRange(double min, double max, bool hasNull) const final;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  std::string toString() const final;
  bool testingEquals(const Filter& other) const final;
  bool isSingleValue() const;

 private:
  // Core testing logic
  template <typename V>
  bool testRange(V value) const;

  // Special handling for float-to-double comparison
  std::string toString(const std::string& name) const;

  // SIMD optimization for floating point tests
  xsimd::batch_bool<T> testFloatingPoints(xsimd::batch<T> values) const;

  const T lower_;
  const T upper_;
  const bool isSingleValue_;
};

// Specializations must come before extern template declarations
template <>
inline std::string FloatingPointRange<double>::toString() const {
  return toString("DoubleRange");
}

template <>
inline std::string FloatingPointRange<float>::toString() const {
  return toString("FloatRange");
}

template <>
inline folly::dynamic FloatingPointRange<float>::serialize() const {
  auto obj = AbstractRange::serializeBase("FloatRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  return obj;
}

template <>
inline folly::dynamic FloatingPointRange<double>::serialize() const {
  auto obj = AbstractRange::serializeBase("DoubleRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  return obj;
}

// Extern template declarations
extern template class FloatingPointRange<float>;
extern template class FloatingPointRange<double>;

using FloatRange = FloatingPointRange<float>;
using DoubleRange = FloatingPointRange<double>;

} // namespace bytedance::bolt::common