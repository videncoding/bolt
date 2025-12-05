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

#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/NegatedFloatingPointRange.h"
#include "bolt/type/filter/NegatedFloatingPointValues.h"
namespace bytedance::bolt::exec {

template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    lessThan(T max, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(),
      static_cast<int64_t>(max) - 1,
      nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    lessThanOrEqual(T max, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(),
      static_cast<int64_t>(max),
      nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    greaterThan(T min, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      static_cast<int64_t>(min) + 1,
      std::numeric_limits<int64_t>::max(),
      nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    greaterThanOrEqual(T min, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      static_cast<int64_t>(min),
      std::numeric_limits<int64_t>::max(),
      nullAllowed);
}

template <typename T>
struct always_false : std::false_type {};

template <typename T>
inline std::unique_ptr<common::Filter> notEqual(
    T val,
    bool nullAllowed = false) {
  if constexpr (is_filter_int_v<T>) {
    return std::make_unique<common::NegatedBigintRange>(val, val, nullAllowed);
  } else if constexpr (std::is_same_v<T, int128_t>) {
    return std::make_unique<common::NegatedHugeintRange>(val, val, nullAllowed);
  } else if constexpr (std::is_same_v<T, float>) {
    return std::make_unique<common::NegatedFloatRange>(
        val, false, false, val, false, false, nullAllowed);
  } else if constexpr (std::is_same_v<T, double>) {
    return std::make_unique<common::NegatedDoubleRange>(
        val, false, false, val, false, false, nullAllowed);
  } else if constexpr (std::is_same_v<T, Timestamp>) {
    return std::make_unique<common::NegatedTimestampRange>(
        val, val, nullAllowed);
  } else if constexpr (std::is_same_v<T, bytedance::bolt::StringView>) {
    return std::make_unique<common::NegatedBytesRange>(
        val, false, false, val, false, false, nullAllowed);
  } else {
    static_assert(always_false<T>::value, "Unsupported type for notEqual");
  }
}

template <typename T>
inline std::enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::Filter>>
notBetween(T lower, T upper, bool nullAllowed = false) {
  return std::make_unique<common::NegatedBigintRange>(
      static_cast<int64_t>(lower), static_cast<int64_t>(upper), nullAllowed);
}

template <typename T>
inline std::enable_if_t<
    std::is_floating_point_v<T>,
    std::unique_ptr<common::FloatingPointRange<T>>>
lessThan(T max, bool nullAllowed = false) {
  return std::make_unique<common::FloatingPointRange<T>>(
      std::numeric_limits<T>::lowest(),
      true,
      true,
      max,
      false,
      true,
      nullAllowed);
}

template <typename T>
inline std::enable_if_t<
    std::is_floating_point_v<T>,
    std::unique_ptr<common::FloatingPointRange<T>>>
lessThanOrEqual(T max, bool nullAllowed = false) {
  return std::make_unique<common::FloatingPointRange<T>>(
      std::numeric_limits<T>::lowest(),
      true,
      true,
      max,
      false,
      false,
      nullAllowed);
}

template <typename T>
inline std::enable_if_t<
    std::is_floating_point_v<T>,
    std::unique_ptr<common::FloatingPointRange<T>>>
greaterThan(T min, bool nullAllowed = false) {
  return std::make_unique<common::FloatingPointRange<T>>(
      min, false, true, std::numeric_limits<T>::max(), true, true, nullAllowed);
}

template <typename T>
inline std::enable_if_t<
    std::is_floating_point_v<T>,
    std::unique_ptr<common::FloatingPointRange<T>>>
greaterThanOrEqual(T min, bool nullAllowed = false) {
  return std::make_unique<common::FloatingPointRange<T>>(
      min,
      false,
      false,
      std::numeric_limits<T>::max(),
      true,
      true,
      nullAllowed);
}

// Integer type overloads - all use BigintRange
template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    equal(T value, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      static_cast<int64_t>(value), static_cast<int64_t>(value), nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<is_filter_int_v<T>, std::unique_ptr<common::BigintRange>>
    between(T min, T max, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      static_cast<int64_t>(min), static_cast<int64_t>(max), nullAllowed);
}

// Helper template for floating point boundary handling
template <typename T>
inline std::
    enable_if_t<std::is_floating_point_v<T>, std::unique_ptr<common::Filter>>
    between(T min, T max, bool nullAllowed = false) {
  return std::make_unique<common::FloatingPointRange<T>>(
      min, // lower bound
      false, // not lower unbounded
      false, // exclusive lower bound
      max, // upper bound
      false, // not upper unbounded
      false, // exclusive upper bound
      nullAllowed);
}

inline std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

inline std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    std::unique_ptr<common::BigintRange> c,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  filters.emplace_back(std::move(c));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

inline std::unique_ptr<common::BytesValues> equal(
    const std::string& value,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(
      std::vector<std::string>{value}, nullAllowed);
}

template <typename T>
inline std::enable_if_t<
    std::is_floating_point_v<T>,
    std::unique_ptr<common::FloatingPointRange<T>>>
equal(T value, bool nullAllowed = false) {
  // Handle NaN - should never equal any value
  if (std::isnan(value)) {
    return std::make_unique<common::FloatingPointRange<T>>(
        T{}, false, true, T{}, false, true, nullAllowed);
  }

  return std::make_unique<common::FloatingPointRange<T>>(
      value, // lower bound
      false, // not lower unbounded
      false, // inclusive lower bound (we want value >= lower)
      value, // upper bound (same as lower for equality)
      false, // not upper unbounded
      false, // inclusive upper bound (we want value <= upper)
      nullAllowed);
}

inline std::unique_ptr<common::BytesRange> between(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::Filter> betweenExclusive(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, true, max, false, true, nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<std::is_floating_point_v<T>, std::unique_ptr<common::Filter>>
    notBetween(T min, T max, bool nullAllowed = false) {
  return std::make_unique<common::NegatedFloatingPointRange<T>>(
      min, // lower bound
      false, // not lower unbounded
      false, // exclusive lower bound
      max, // upper bound
      false, // not upper unbounded
      false, // exclusive upper bound
      nullAllowed);
}

inline std::unique_ptr<common::Filter> notBetween(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::Filter> notBetween(
    const Timestamp& min,
    const Timestamp& max,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedTimestampRange>(min, max, nullAllowed);
}

inline std::unique_ptr<common::Filter> notBetweenExclusive(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesRange>(
      min, false, true, max, false, true, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> lessThanOrEqual(
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      "", true, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> lessThan(
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      "", true, false, max, false, true, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> greaterThanOrEqual(
    const std::string& min,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, false, "", true, false, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> greaterThan(
    const std::string& min,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, true, "", true, false, nullAllowed);
}

inline std::unique_ptr<common::Filter> in(
    const std::vector<int64_t>& values,
    bool nullAllowed = false) {
  return common::createBigintValues(values, nullAllowed);
}

inline std::unique_ptr<common::Filter> notIn(
    const std::vector<int64_t>& values,
    bool nullAllowed = false) {
  return common::createNegatedBigintValues(values, nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<std::is_floating_point_v<T>, std::unique_ptr<common::Filter>>
    in(const std::vector<T>& values, bool nullAllowed = false) {
  auto valuesCopy = values;
  return std::make_unique<common::FloatingPointValues<T>>(
      valuesCopy, nullAllowed);
}

template <typename T>
inline std::
    enable_if_t<std::is_floating_point_v<T>, std::unique_ptr<common::Filter>>
    notIn(const std::vector<T>& values, bool nullAllowed = false) {
  return std::make_unique<common::NegatedFloatingPointValues<T>>(
      values, nullAllowed);
}

inline std::unique_ptr<common::Filter> in(
    const std::vector<std::string>& values,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(values, nullAllowed);
}

inline std::unique_ptr<common::Filter> notIn(
    const std::vector<std::string>& values,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesValues>(values, nullAllowed);
}

inline std::unique_ptr<common::BoolValue> boolEqual(
    bool value,
    bool nullAllowed = false) {
  return std::make_unique<common::BoolValue>(value, nullAllowed);
}

inline std::unique_ptr<common::IsNull> isNull() {
  return std::make_unique<common::IsNull>();
}

inline std::unique_ptr<common::IsNotNull> isNotNull() {
  return std::make_unique<common::IsNotNull>();
}

inline std::unique_ptr<common::Like> like(
    const std::string& value,
    bool nullAllowed = false) {
  return std::make_unique<common::Like>(value, nullAllowed);
}

inline std::unique_ptr<common::Filter> orFilter(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b,
    bool nanAllowed = false) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  if (a == nullptr) {
    return b;
  } else if (b == nullptr) {
    return a;
  }

  bool nullAllowed = a->testNull() || b->testNull();
  filters.push_back(std::move(a));
  filters.push_back(std::move(b));

  return std::make_unique<common::MultiRange>(
      std::move(filters), nullAllowed, nanAllowed);
}

inline std::unique_ptr<common::HugeintRange> lessThan(
    int128_t max,
    bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(
      std::numeric_limits<int128_t>::min(), max - 1, nullAllowed);
}

inline std::unique_ptr<common::HugeintRange> lessThanOrEqual(
    int128_t max,
    bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(
      std::numeric_limits<int128_t>::min(), max, nullAllowed);
}

inline std::unique_ptr<common::HugeintRange> greaterThan(
    int128_t min,
    bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(
      min + 1, std::numeric_limits<int128_t>::max(), nullAllowed);
}

inline std::unique_ptr<common::HugeintRange> greaterThanOrEqual(
    int128_t min,
    bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(
      min, std::numeric_limits<int128_t>::max(), nullAllowed);
}

inline std::unique_ptr<common::HugeintRange> equal(
    int128_t value,
    bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(value, value, nullAllowed);
}

inline std::unique_ptr<common::Filter>
between(int128_t min, int128_t max, bool nullAllowed = false) {
  return std::make_unique<common::HugeintRange>(min, max, nullAllowed);
}

inline std::unique_ptr<common::Filter> equal(
    const Timestamp& value,
    bool nullAllowed = false) {
  return std::make_unique<common::TimestampRange>(value, value, nullAllowed);
}

inline std::unique_ptr<common::TimestampRange>
between(const Timestamp& min, const Timestamp& max, bool nullAllowed = false) {
  return std::make_unique<common::TimestampRange>(min, max, nullAllowed);
}

inline std::unique_ptr<common::TimestampRange> lessThan(
    Timestamp max,
    bool nullAllowed = false) {
  --max;
  return std::make_unique<common::TimestampRange>(
      std::numeric_limits<Timestamp>::min(), max, nullAllowed);
}

inline std::unique_ptr<common::TimestampRange> lessThanOrEqual(
    const Timestamp& max,
    bool nullAllowed = false) {
  return std::make_unique<common::TimestampRange>(
      std::numeric_limits<Timestamp>::min(), max, nullAllowed);
}

inline std::unique_ptr<common::TimestampRange> greaterThan(
    Timestamp min,
    bool nullAllowed = false) {
  ++min;
  return std::make_unique<common::TimestampRange>(
      min, std::numeric_limits<Timestamp>::max(), nullAllowed);
}

inline std::unique_ptr<common::TimestampRange> greaterThanOrEqual(
    const Timestamp& min,
    bool nullAllowed = false) {
  return std::make_unique<common::TimestampRange>(
      min, std::numeric_limits<Timestamp>::max(), nullAllowed);
}
} // namespace bytedance::bolt::exec