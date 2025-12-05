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

#include "bolt/type/filter/FloatingPointRange.h"
#include <fmt/format.h>
namespace bytedance::bolt::common {

template <typename T>
FloatingPointRange<T>::FloatingPointRange(
    T lower,
    bool lowerUnbounded,
    bool lowerExclusive,
    T upper,
    bool upperUnbounded,
    bool upperExclusive,
    bool nullAllowed)
    : AbstractRange(
          lowerUnbounded,
          lowerExclusive,
          upperUnbounded,
          upperExclusive,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleRange
                                      : FilterKind::kFloatRange),
      lower_(lower),
      upper_(upper),
      isSingleValue_(lower_ == upper_) {
  BOLT_CHECK(lowerUnbounded || !std::isnan(lower_));
  BOLT_CHECK(upperUnbounded || !std::isnan(upper_));
}

template <typename T>
FloatingPointRange<T>::FloatingPointRange(
    const FloatingPointRange& other,
    bool nullAllowed)
    : AbstractRange(
          other.lowerUnbounded_,
          other.lowerExclusive_,
          other.upperUnbounded_,
          other.upperExclusive_,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleRange
                                      : FilterKind::kFloatRange),
      lower_(other.lower_),
      upper_(other.upper_),
      isSingleValue_(lower_ == upper_) {
  BOLT_CHECK(lowerUnbounded_ || !std::isnan(lower_));
  BOLT_CHECK(upperUnbounded_ || !std::isnan(upper_));
}

template <typename T>
bool FloatingPointRange<T>::isSingleValue() const {
  return isSingleValue_;
}

template <typename T>
template <typename V>
bool FloatingPointRange<T>::testRange(V value) const {
  if (std::isnan(value)) {
    return false;
  }

  // Split range case (e.g. [5,3] is treated as (-∞,3] ∪ [5,+∞))
  if (!lowerUnbounded_ && !upperUnbounded_ && upper_ < lower_) {
    return (upperExclusive_ ? value < upper_ : value <= upper_) ||
        (lowerExclusive_ ? value > lower_ : value >= lower_);
  }

  // Test bounds
  if (!lowerUnbounded_) {
    if (value < lower_ || (lowerExclusive_ && value == lower_)) {
      return false;
    }
  }

  if (!upperUnbounded_) {
    if (value > upper_ || (upperExclusive_ && value == upper_)) {
      return false;
    }
  }

  return true;
}

template <typename T>
bool FloatingPointRange<T>::testDouble(double value) const {
  return testRange(static_cast<T>(value));
}

template <typename T>
bool FloatingPointRange<T>::testFloat(float value) const {
  return testRange(value);
}

template <typename T>
xsimd::batch_bool<double> FloatingPointRange<T>::testValues(
    xsimd::batch<double> values) const {
  if constexpr (std::is_same_v<T, double>) {
    return testFloatingPoints(values);
  } else {
    return Filter::testValues(values);
  }
}

template <typename T>
xsimd::batch_bool<float> FloatingPointRange<T>::testValues(
    xsimd::batch<float> values) const {
  if constexpr (std::is_same_v<T, float>) {
    return testFloatingPoints(values);
  } else {
    return Filter::testValues(values);
  }
}

template <typename T>
xsimd::batch_bool<T> FloatingPointRange<T>::testFloatingPoints(
    xsimd::batch<T> values) const {
  xsimd::batch_bool<T> result;

  if (!lowerUnbounded_ && !upperUnbounded_ && upper_ < lower_) {
    auto allLower = xsimd::broadcast<T>(lower_);
    auto allUpper = xsimd::broadcast<T>(upper_);
    return (upperExclusive_ ? (values < allUpper) : (values <= allUpper)) ||
        (lowerExclusive_ ? (values > allLower) : (values >= allLower));
  }

  if (!lowerUnbounded_) {
    auto allLower = xsimd::broadcast<T>(lower_);
    if (lowerExclusive_) {
      result = allLower < values;
    } else {
      result = allLower <= values;
    }
    if (!upperUnbounded_) {
      auto allUpper = xsimd::broadcast<T>(upper_);
      if (upperExclusive_) {
        result = result & (values < allUpper);
      } else {
        result = result & (values <= allUpper);
      }
    }
  } else {
    auto allUpper = xsimd::broadcast<T>(upper_);
    if (upperExclusive_) {
      result = values < allUpper;
    } else {
      result = values <= allUpper;
    }
  }
  return result;
}

template <typename T>
bool FloatingPointRange<T>::testDoubleRange(
    double min,
    double max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  return !(min > upper_ || max < lower_);
}

template <typename T>
std::unique_ptr<Filter> FloatingPointRange<T>::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<FloatingPointRange<T>>(
          lower_,
          lowerUnbounded_,
          lowerExclusive_,
          upper_,
          upperUnbounded_,
          upperExclusive_,
          false);
    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      auto otherRange = static_cast<const FloatingPointRange<T>*>(other);

      auto lower = std::max(lower_, otherRange->lower_);
      auto upper = std::min(upper_, otherRange->upper_);

      auto bothLowerUnbounded = lowerUnbounded_ && otherRange->lowerUnbounded_;
      auto bothUpperUnbounded = upperUnbounded_ && otherRange->upperUnbounded_;

      auto lowerExclusive = !bothLowerUnbounded &&
          (!testDouble(lower) || !other->testDouble(lower));
      auto upperExclusive = !bothUpperUnbounded &&
          (!testDouble(upper) || !other->testDouble(upper));

      if (lower > upper || (lower == upper && lowerExclusive_)) {
        if (bothNullAllowed) {
          return std::make_unique<IsNull>();
        }
        return std::make_unique<AlwaysFalse>();
      }

      return std::make_unique<FloatingPointRange<T>>(
          lower,
          bothLowerUnbounded,
          lowerExclusive,
          upper,
          bothUpperUnbounded,
          upperExclusive,
          bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

template <typename T>
bool FloatingPointRange<T>::testingEquals(const Filter& other) const {
  if (kind() != other.kind()) {
    return false;
  }
  auto otherRange = static_cast<const FloatingPointRange<T>&>(other);
  return lowerUnbounded_ == otherRange.lowerUnbounded_ &&
      upperUnbounded_ == otherRange.upperUnbounded_ &&
      lowerExclusive_ == otherRange.lowerExclusive_ &&
      upperExclusive_ == otherRange.upperExclusive_ &&
      nullAllowed_ == otherRange.nullAllowed_ && lower_ == otherRange.lower_ &&
      upper_ == otherRange.upper_;
}

template <typename T>
std::string FloatingPointRange<T>::toString(const std::string& name) const {
  return fmt::format(
      "{}: {}{}, {}{} {}",
      name,
      (lowerExclusive_ || lowerUnbounded_) ? "(" : "[",
      lowerUnbounded_ ? "-inf" : std::to_string(lower_),
      upperUnbounded_ ? "+inf" : std::to_string(upper_),
      (upperExclusive_ || upperUnbounded_) ? ")" : "]",
      nullAllowed_ ? "with nulls" : "no nulls");
}

template <typename T>
std::unique_ptr<Filter> FloatingPointRange<T>::clone(
    std::optional<bool> nullAllowed) const {
  if (nullAllowed) {
    return std::make_unique<FloatingPointRange<T>>(*this, nullAllowed.value());
  }
  return std::make_unique<FloatingPointRange<T>>(*this);
}

// Explicit template instantiations
template class FloatingPointRange<float>;
template class FloatingPointRange<double>;

} // namespace bytedance::bolt::common