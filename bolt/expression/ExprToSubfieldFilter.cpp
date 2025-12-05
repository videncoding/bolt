/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <cstdint>
#include <memory>

#include "bolt/core/Expressions.h"
#include "bolt/core/ITypedExpr.h"
#include "bolt/expression/CoalesceExpr.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/expression/SingleSubfieldExtractor.h"
#include "bolt/type/Filter.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/filter/Cast.h"
#include "bolt/type/filter/FilterCreator.h"
#include "bolt/type/filter/MapSubscriptFilter.h"
using namespace bytedance::bolt;
namespace bytedance::bolt::exec {

namespace {

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

const core::CastTypedExpr* asCast(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CastTypedExpr*>(expr);
}

struct FunctionInfo {
  TypePtr sourceType;
  TypePtr targetType;
  std::string functionName;
  size_t fieldDepth;

  static std::optional<FunctionInfo> tryExtractFromExpr(
      const core::TypedExprPtr& expr);

  static inline const std::unordered_set<std::string> filterKeys = {
      "eq",
      "neq",
      "lt",
      "gt",
      "lte",
      "gte",
      "between",
      "presto.default.eq",
      "presto.default.neq",
      "presto.default.lt",
      "presto.default.gt",
      "presto.default.lte",
      "presto.default.gte",
      "presto.default.between",
      "in",
      "is_null",
      "cast",
      "subscript",
      "presto.default.subscript",
      "element_at",
      "presto.default.element_at"};
};

std::optional<FunctionInfo> FunctionInfo::tryExtractFromExpr(
    const core::TypedExprPtr& expr) {
  SingleSubfieldExtractor extractor;
  auto [chainOpt, depth] = extractor.fieldTypedExpr(expr.get());
  if (chainOpt == nullptr) {
    return std::nullopt;
  }

  if (auto callExpr = asCall(expr.get())) {
    if ((callExpr->name() == "subscript" ||
         callExpr->name() == "presto.default.subscript" ||
         callExpr->name() == "element_at" ||
         callExpr->name() == "presto.default.element_at") &&
        filterKeys.find(callExpr->name()) != filterKeys.end()) {
      return FunctionInfo{
          chainOpt->type(), callExpr->type(), callExpr->name(), depth};
    } else if (filterKeys.find(callExpr->name()) == filterKeys.end()) {
      return FunctionInfo{
          chainOpt->type(), callExpr->type(), callExpr->name(), depth};
    }
  } else if (auto castExpr = asCast(expr.get())) {
    return FunctionInfo{chainOpt->type(), castExpr->type(), "cast", depth};
  }
  return std::nullopt;
};

VectorPtr toConstant(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  auto exprSet = evaluator->compile(expr);
  if (!exprSet->exprs()[0]->isConstant()) {
    return nullptr;
  }
  RowVector input(
      evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const BoltUserError&) {
    return nullptr;
  }
  return result;
}

template <typename T>
T singleValue(const VectorPtr& vector) {
  auto simpleVector = vector->as<SimpleVector<T>>();
  BOLT_CHECK_NOT_NULL(simpleVector);
  return simpleVector->valueAt(0);
}

} // namespace

bool toSubfield(const core::ITypedExpr* field, common::Subfield& subfield) {
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  const core::ITypedExpr* current = field;

  // Traverse through cast expressions
  while (current) {
    switch (current->typedExprKind()) {
      case core::TypedExprKind::kFieldAccess: {
        auto* fieldAccess =
            static_cast<const core::FieldAccessTypedExpr*>(current);
        path.push_back(std::make_unique<common::Subfield::NestedField>(
            fieldAccess->name()));
        if (current->inputs().empty()) {
          std::reverse(path.begin(), path.end());
          subfield = common::Subfield(std::move(path));
          return true;
        }
        if (current->inputs().size() != 1) {
          return false;
        }
        current = current->inputs()[0].get();
        break;
      }

      case core::TypedExprKind::kDereference: {
        auto* dereference =
            static_cast<const core::DereferenceTypedExpr*>(current);
        const auto& name = dereference->name();
        if (name.empty()) {
          return false;
        }
        path.push_back(std::make_unique<common::Subfield::NestedField>(name));
        if (current->inputs().size() != 1) {
          return false;
        }
        current = current->inputs()[0].get();
        break;
      }

      case core::TypedExprKind::kCall: {
        auto* callExpr = static_cast<const core::CallTypedExpr*>(current);
        if (callExpr->name() == "cast" && callExpr->inputs().size() == 1) {
          current = callExpr->inputs()[0].get();
          break;
        }
        // Add subscript support for maps
        if ((callExpr->name() == "subscript" ||
             callExpr->name() == "presto.default.subscript") &&
            callExpr->inputs().size() == 2) {
          // Handle map subscript: map[key]
          auto mapExpr = callExpr->inputs()[0].get();
          auto keyExpr = callExpr->inputs()[1].get();

          // Try to extract constant key for map subscript
          // TODO: Handle non-constant key expression
          if (auto constantExpr =
                  dynamic_cast<const core::ConstantTypedExpr*>(keyExpr)) {
            auto keyVariant = constantExpr->value();
            if (keyVariant.hasValue()) {
              if (keyVariant.kind() == TypeKind::VARCHAR) {
                // String key for map
                auto stringKey = keyVariant.value<TypeKind::VARCHAR>();
                path.push_back(
                    std::make_unique<common::Subfield::StringSubscript>(
                        stringKey));
              } else if (keyVariant.kind() == TypeKind::BIGINT) {
                // Integer key for map (or array index)
                auto intKey = keyVariant.value<TypeKind::BIGINT>();
                path.push_back(
                    std::make_unique<common::Subfield::LongSubscript>(intKey));
              }
              current = callExpr->inputs()[0].get();
              break;
            }
          }
          // If key is not constant, we still want to form a subfield path.
          // We use a placeholder name for non-constant keys.
          path.push_back(std::make_unique<common::Subfield::NestedField>(
              "$non_constant_key$"));
          current = callExpr->inputs()[0].get();
          break;
        }
        return false;
      }

      case core::TypedExprKind::kCast: {
        auto* castExpr = static_cast<const core::CastTypedExpr*>(current);
        if (castExpr->inputs().size() == 1) {
          current = castExpr->inputs()[0].get();
          break;
        }
        return false;
      }

      case core::TypedExprKind::kInput: {
        std::reverse(path.begin(), path.end());
        subfield = common::Subfield(std::move(path));
        return true;
      }

      default:
        return false;
    }
  }

  if (!path.empty()) {
    std::reverse(path.begin(), path.end());
    subfield = common::Subfield(std::move(path));
    return true;
  }

  return false;
}

common::BigintRange* asBigintRange(std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr && filter->kind() == common::FilterKind::kBigintRange) {
    return static_cast<common::BigintRange*>(filter.get());
  }
  return nullptr;
}

common::BigintMultiRange* asBigintMultiRange(
    std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr &&
      filter->kind() == common::FilterKind::kBigintMultiRange) {
    return static_cast<common::BigintMultiRange*>(filter.get());
  }
  return nullptr;
}

common::FloatingPointRange<float>* asFloatRange(
    std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr && filter->kind() == common::FilterKind::kFloatRange) {
    return static_cast<common::FloatingPointRange<float>*>(filter.get());
  }
  return nullptr;
}

common::FloatingPointRange<double>* asDoubleRange(
    std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr && filter->kind() == common::FilterKind::kDoubleRange) {
    return static_cast<common::FloatingPointRange<double>*>(filter.get());
  }
  return nullptr;
}

common::FloatingPointMultiRange<float>* asFloatMultiRange(
    std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr &&
      filter->kind() == common::FilterKind::kFloatMultiRange) {
    return static_cast<common::FloatingPointMultiRange<float>*>(filter.get());
  }
  return nullptr;
}

common::FloatingPointMultiRange<double>* asDoubleMultiRange(
    std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr &&
      filter->kind() == common::FilterKind::kDoubleMultiRange) {
    return static_cast<common::FloatingPointMultiRange<double>*>(filter.get());
  }
  return nullptr;
}

template <typename T, typename U>
std::unique_ptr<T> asUniquePtr(std::unique_ptr<U> ptr) {
  return std::unique_ptr<T>(static_cast<T*>(ptr.release()));
}

std::unique_ptr<common::Filter> makeOrFilter(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b) {
  // Handle BigInt cases first
  if (asBigintRange(a) && asBigintRange(b)) {
    return bigintOr(
        asUniquePtr<common::BigintRange>(std::move(a)),
        asUniquePtr<common::BigintRange>(std::move(b)));
  }

  if (asBigintRange(a) && asBigintMultiRange(b)) {
    const auto& ranges = asBigintMultiRange(b)->ranges();
    std::vector<std::unique_ptr<common::BigintRange>> newRanges;
    newRanges.emplace_back(asUniquePtr<common::BigintRange>(std::move(a)));
    for (const auto& range : ranges) {
      newRanges.emplace_back(asUniquePtr<common::BigintRange>(range->clone()));
    }

    std::sort(
        newRanges.begin(), newRanges.end(), [](const auto& a, const auto& b) {
          return a->lower() < b->lower();
        });

    return std::make_unique<common::BigintMultiRange>(
        std::move(newRanges), false);
  }

  if (asBigintMultiRange(a) && asBigintRange(b)) {
    return makeOrFilter(std::move(b), std::move(a));
  }

  // Handle Float cases
  if (asFloatRange(a) && asFloatRange(b)) {
    std::vector<std::unique_ptr<common::FloatingPointRange<float>>> ranges;
    ranges.emplace_back(
        asUniquePtr<common::FloatingPointRange<float>>(std::move(a)));
    ranges.emplace_back(
        asUniquePtr<common::FloatingPointRange<float>>(std::move(b)));
    return std::make_unique<common::FloatingPointMultiRange<float>>(
        std::move(ranges), false);
  }

  if (asFloatRange(a) && asFloatMultiRange(b)) {
    const auto& ranges = asFloatMultiRange(b)->ranges();
    std::vector<std::unique_ptr<common::FloatingPointRange<float>>> newRanges;
    newRanges.emplace_back(
        asUniquePtr<common::FloatingPointRange<float>>(std::move(a)));
    for (const auto& range : ranges) {
      newRanges.emplace_back(std::unique_ptr<common::FloatingPointRange<float>>(
          static_cast<common::FloatingPointRange<float>*>(
              range->clone().release())));
    }
    return std::make_unique<common::FloatingPointMultiRange<float>>(
        std::move(newRanges), false);
  }

  if (asFloatMultiRange(a) && asFloatRange(b)) {
    return makeOrFilter(std::move(b), std::move(a));
  }

  // Handle Double cases
  if (asDoubleRange(a) && asDoubleRange(b)) {
    std::vector<std::unique_ptr<common::FloatingPointRange<double>>> ranges;
    ranges.emplace_back(
        asUniquePtr<common::FloatingPointRange<double>>(std::move(a)));
    ranges.emplace_back(
        asUniquePtr<common::FloatingPointRange<double>>(std::move(b)));
    return std::make_unique<common::FloatingPointMultiRange<double>>(
        std::move(ranges), false);
  }

  if (asDoubleRange(a) && asDoubleMultiRange(b)) {
    const auto& ranges = asDoubleMultiRange(b)->ranges();
    std::vector<std::unique_ptr<common::FloatingPointRange<double>>> newRanges;
    newRanges.emplace_back(
        asUniquePtr<common::FloatingPointRange<double>>(std::move(a)));
    for (const auto& range : ranges) {
      newRanges.emplace_back(
          std::unique_ptr<common::FloatingPointRange<double>>(
              static_cast<common::FloatingPointRange<double>*>(
                  range->clone().release())));
    }
    return std::make_unique<common::FloatingPointMultiRange<double>>(
        std::move(newRanges), false);
  }

  if (asDoubleMultiRange(a) && asDoubleRange(b)) {
    return makeOrFilter(std::move(b), std::move(a));
  }

  return orFilter(std::move(a), std::move(b));
}

std::unique_ptr<common::Filter> makeLessThanOrEqualFilter(
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThanOrEqual(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThanOrEqual(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThanOrEqual(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThanOrEqual(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThanOrEqual(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanOrEqual(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanOrEqual(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThanOrEqual(singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return lessThanOrEqual(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeLessThanFilter(
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThan(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThan(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThan(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThan(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThan(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThan(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThan(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThan(singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return lessThan(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeGreaterThanOrEqualFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThanOrEqual(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThanOrEqual(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThanOrEqual(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThanOrEqual(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThanOrEqual(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanOrEqual(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanOrEqual(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThanOrEqual(singleValue<StringView>(lower));
    case TypeKind::TIMESTAMP:
      return greaterThanOrEqual(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeGreaterThanFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThan(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThan(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThan(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThan(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThan(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThan(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThan(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThan(singleValue<StringView>(lower));
    case TypeKind::TIMESTAMP:
      return greaterThan(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }
  switch (value->typeKind()) {
    case TypeKind::BOOLEAN:
      return boolEqual(singleValue<bool>(value));
    case TypeKind::TINYINT:
      return equal(singleValue<int8_t>(value));
    case TypeKind::SMALLINT:
      return equal(singleValue<int16_t>(value));
    case TypeKind::INTEGER:
      return equal(singleValue<int32_t>(value));
    case TypeKind::BIGINT:
      return equal(singleValue<int64_t>(value));
    case TypeKind::DOUBLE:
      return equal(singleValue<double>(value));
    case TypeKind::REAL:
      return equal(singleValue<float>(value));
    case TypeKind::HUGEINT:
      return equal(singleValue<int128_t>(value));
    case TypeKind::VARCHAR:
      return equal(singleValue<StringView>(value));
    case TypeKind::TIMESTAMP:
      return equal(singleValue<Timestamp>(value));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeNotEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }

  switch (value->typeKind()) {
    case TypeKind::TINYINT:
      return notEqual(singleValue<int8_t>(value));
    case TypeKind::SMALLINT:
      return notEqual(singleValue<int16_t>(value));
    case TypeKind::INTEGER:
      return notEqual(singleValue<int32_t>(value));
    case TypeKind::BIGINT:
      return notEqual(singleValue<int64_t>(value));
    case TypeKind::HUGEINT:
      return notEqual(singleValue<int128_t>(value));
    case TypeKind::DOUBLE:
      return notEqual(singleValue<double>(value));
    case TypeKind::REAL:
      return notEqual(singleValue<float>(value));
    case TypeKind::TIMESTAMP:
      return notEqual(singleValue<Timestamp>(value));
    case TypeKind::VARCHAR:
      return notEqual(singleValue<StringView>(value));
    default:
      return nullptr;
  }
}

template <typename T>
std::vector<T> toFloatingPointList(
    const VectorPtr& elements,
    vector_size_t offset,
    vector_size_t size) {
  std::vector<T> result;
  result.reserve(size);
  auto values = elements->as<SimpleVector<T>>();
  for (vector_size_t i = 0; i < size; ++i) {
    if (!values->isNullAt(offset + i)) {
      result.push_back(values->valueAt(offset + i));
    }
  }
  return result;
}

template <typename T>
std::vector<int64_t>
toInt64List(const VectorPtr& vector, vector_size_t start, vector_size_t size) {
  auto ints = vector->as<SimpleVector<T>>();
  std::vector<int64_t> values;
  for (auto i = 0; i < size; i++) {
    values.push_back(ints->valueAt(start + i));
  }
  return values;
}

std::unique_ptr<common::Filter> makeInFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto vector = toConstant(expr, evaluator);
  if (!(vector && vector->type()->isArray())) {
    return nullptr;
  }

  auto arrayVector = vector->valueVector()->as<ArrayVector>();
  auto index = vector->as<ConstantVector<ComplexType>>()->index();
  auto offset = arrayVector->offsetAt(index);
  auto size = arrayVector->sizeAt(index);
  auto elements = arrayVector->elements();

  // Process this kind of query:
  // SELECT * FROM mytable WHERE CAST(col AS DOUBLE) IN (NULL)
  auto isNullVector = arrayVector->isNullAt(index);
  if (isNullVector) {
    return isNull();
  }

  auto elementType = arrayVector->type()->asArray().elementType();
  switch (elementType->kind()) {
    // Don't combine TINYINT/SMALLINT/INTEGER/BIGINT, since
    // toInt64List would use the type parameter to extract a
    // SimpleVector<T>. Wrong type parameter would fail the execution.
    case TypeKind::TINYINT: {
      auto values = toInt64List<int8_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::SMALLINT: {
      auto values = toInt64List<int16_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::INTEGER: {
      auto values = toInt64List<int32_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::BIGINT: {
      auto values = toInt64List<int64_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::DOUBLE: {
      auto values = toFloatingPointList<double>(elements, offset, size);
      // Create filter using FloatingPointValues
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::REAL: {
      auto values = toFloatingPointList<float>(elements, offset, size);
      // Sort values to find min/max for range
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::VARCHAR: {
      auto stringElements = elements->as<SimpleVector<StringView>>();
      std::vector<std::string> values;
      values.reserve(size);
      for (auto i = 0; i < size; i++) {
        if (!stringElements->isNullAt(offset + i)) {
          values.push_back(stringElements->valueAt(offset + i).str());
        }
      }
      return negated ? notIn(values) : in(values);
    }
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeBetweenFilter(
    const core::TypedExprPtr& lowerExpr,
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return negated
          ? notBetween(singleValue<int8_t>(lower), singleValue<int8_t>(upper))
          : between(singleValue<int8_t>(lower), singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return negated
          ? notBetween(singleValue<int16_t>(lower), singleValue<int16_t>(upper))
          : between(singleValue<int16_t>(lower), singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return negated
          ? notBetween(singleValue<int32_t>(lower), singleValue<int32_t>(upper))
          : between(singleValue<int32_t>(lower), singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return negated
          ? notBetween(singleValue<int64_t>(lower), singleValue<int64_t>(upper))
          : between(singleValue<int64_t>(lower), singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return negated
          ? notBetween(singleValue<double>(lower), singleValue<double>(upper))
          : between(singleValue<double>(lower), singleValue<double>(upper));
    case TypeKind::REAL:
      return negated
          ? notBetween(singleValue<float>(lower), singleValue<float>(upper))
          : between(singleValue<float>(lower), singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return negated
          ? notBetween(
                singleValue<StringView>(lower), singleValue<StringView>(upper))
          : between(
                singleValue<StringView>(lower), singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return negated
          ? notBetween(
                singleValue<Timestamp>(lower), singleValue<Timestamp>(upper))
          : between(
                singleValue<Timestamp>(lower), singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return nullptr;
  }

  // Try building filter using the new ColumnFilterBuilder
  auto result = ColumnFilterBuilder::tryBuild(call, evaluator, negated);

  // Handle successful filter creation
  if (result.subfield && result.filter) {
    subfield = std::move(*result.subfield);
    return std::move(result.filter);
  }

  // Handle case where we got subfield but no filter
  if (result.subfield) {
    subfield = std::move(*result.subfield);
    return nullptr;
  }

  return nullptr;
}

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  if (auto call = asCall(expr.get())) {
    if (call->name() == "or") {
      auto left = toSubfieldFilter(call->inputs()[0], evaluator);
      auto right = toSubfieldFilter(call->inputs()[1], evaluator);
      BOLT_CHECK(left.first == right.first);
      return {
          std::move(left.first),
          makeOrFilter(std::move(left.second), std::move(right.second))};
    }
    common::Subfield subfield;
    std::unique_ptr<common::Filter> filter;
    if (call->name() == "not") {
      if (auto* inner = asCall(call->inputs()[0].get())) {
        filter = leafCallToSubfieldFilter(*inner, subfield, evaluator, true);
      }
    } else {
      filter = leafCallToSubfieldFilter(*call, subfield, evaluator, false);
    }

    if (filter != nullptr || !subfield.valid()) {
      // For invalid subfield, it's 'to_string()' will return empty string.
      // QueryPlan will ignore it.
      return std::make_pair(std::move(subfield), std::move(filter));
    }
  }
  BOLT_UNSUPPORTED(
      "Unsupported expression for range filter: {}", expr->toString());
}

std::optional<common::Subfield> ColumnFilterBuilder::extractColumnAccess(
    const core::ITypedExpr* expr) {
  SingleSubfieldExtractor extractor;
  auto extractResult = extractor.extract(expr);
  if (!extractResult.has_value()) {
    return std::nullopt;
  }

  auto [chainOpt, depth] = extractResult.value();
  if (chainOpt.empty()) {
    return std::nullopt;
  }

  // Build Subfield path
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  path.reserve(chainOpt.size());
  for (auto& name : chainOpt) {
    path.push_back(std::make_unique<common::Subfield::NestedField>(name));
  }
  return common::Subfield(std::move(path));
}

bool checkTypeCompatibility(const TypePtr& leftType, const TypePtr& rightType) {
  // If right type is array, compare with its element type
  if (rightType->isArray()) {
    return leftType->kindEquals(rightType->asArray().elementType());
  }

  // Otherwise do direct type comparison
  return leftType->kindEquals(rightType);
}

SubfieldFilterResult ColumnFilterBuilder::tryBuild(
    const core::CallTypedExpr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  // Early return for invalid inputs
  if (expr.inputs().empty()) {
    return SubfieldFilterResult::unsupported();
  }

  // Extract column access from left and fallback to right side.
  auto columnAccess = extractColumnAccess(expr.inputs()[0].get());
  if (!columnAccess && expr.inputs().size() > 1) {
    columnAccess = extractColumnAccess(expr.inputs()[1].get());
  }

  if (!columnAccess) {
    return SubfieldFilterResult::unsupported();
  }

  // Rest of the function remains the same...
  auto it = filterBuilders_.find(expr.name());
  if (it == filterBuilders_.end() ||
      (expr.inputs().size() == 2 &&
       !checkTypeCompatibility(
           expr.inputs()[0]->type(), expr.inputs()[1]->type()))) {
    return SubfieldFilterResult::unsupported();
  }

  auto result = it->second(expr, *columnAccess, evaluator, negated);
  if (!result.filter && !result.subfield) {
    return SubfieldFilterResult::unsupported();
  }

  return result;
}

SubfieldFilterResult ColumnFilterBuilder::buildComparisonFilter(
    const core::CallTypedExpr& expr,
    const common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (expr.inputs().size() < 2) {
    return SubfieldFilterResult::unsupported();
  }

  const auto leftSide = expr.inputs()[0];
  const auto rightSide = expr.inputs()[1];

  auto leftColumnAccess = extractColumnAccess(leftSide.get());
  auto rightColumnAccess = extractColumnAccess(rightSide.get());

  // First determine which side has the column access
  bool leftHasColumn = extractColumnAccess(leftSide.get()).has_value();
  bool rightHasColumn = extractColumnAccess(rightSide.get()).has_value();

  if (!leftHasColumn && !rightHasColumn) {
    return SubfieldFilterResult::unsupported();
  }

  // Determine field and input sides
  bool swapped = rightHasColumn;
  const auto& fieldSide = swapped ? rightSide : leftSide;
  core::TypedExprPtr inputExpr = swapped ? expr.inputs()[0] : expr.inputs()[1];

  // Handle constant evaluation
  if (inputExpr->typedExprKind() == core::kConstant) {
    auto constantVector = toConstant(inputExpr, evaluator);
    if (!constantVector) {
      return SubfieldFilterResult::unsupported();
    }
    inputExpr = std::make_shared<core::ConstantTypedExpr>(constantVector);
  }

  // Generate the appropriate filter
  const auto& name = expr.name();
  std::unique_ptr<common::Filter> filter;

  if (isEqualityFn(name)) {
    filter = negated ? makeNotEqualFilter(inputExpr, evaluator)
                     : makeEqualFilter(inputExpr, evaluator);
  } else if (isNotEqualFn(name)) {
    filter = negated ? makeEqualFilter(inputExpr, evaluator)
                     : makeNotEqualFilter(inputExpr, evaluator);
  } else {
    using FilterMaker = std::unique_ptr<common::Filter> (*)(
        const core::TypedExprPtr&, core::ExpressionEvaluator*);

    FilterMaker lt = makeLessThanFilter;
    FilterMaker gt = makeGreaterThanFilter;
    FilterMaker lte = makeLessThanOrEqualFilter;
    FilterMaker gte = makeGreaterThanOrEqualFilter;

    if (swapped) {
      std::swap(lt, gt);
      std::swap(lte, gte);
    }
    // Negation complements the comparison (e.g. !(<) -> >=).
    if (negated) {
      std::swap(lt, gte);
      std::swap(gt, lte);
    }

    if (isLessThanFn(name)) {
      filter = lt(inputExpr, evaluator);
    } else if (isGreaterThanFn(name)) {
      filter = gt(inputExpr, evaluator);
    } else if (isLessThanOrEqualFn(name)) {
      filter = lte(inputExpr, evaluator);
    } else if (isGreaterThanOrEqualFn(name)) {
      filter = gte(inputExpr, evaluator);
    }
  }

  if (!filter) {
    return SubfieldFilterResult::unsupported();
  }

  if (auto functionInfo = FunctionInfo::tryExtractFromExpr(fieldSide)) {
    // Cast operator is much faster than ExprSetFilter.
    if (functionInfo->functionName == "cast" && functionInfo->fieldDepth == 1) {
      filter = common::createCastFilter(
          functionInfo->sourceType,
          functionInfo->targetType,
          std::move(filter));
    } else if (
        (functionInfo->functionName == "subscript" ||
         functionInfo->functionName == "presto.default.subscript" ||
         functionInfo->functionName == "element_at" ||
         functionInfo->functionName == "presto.default.element_at") &&
        functionInfo->fieldDepth == 1) {
      // For map subscript expressions, create appropriate filters
      // based on the map value type
      if (functionInfo->sourceType->isMap()) {
        auto mapType = functionInfo->sourceType->asMap();

        // Extract the key from the subscript expression
        std::string key;
        if (auto* callExpr =
                dynamic_cast<const core::CallTypedExpr*>(fieldSide.get())) {
          BOLT_CHECK_EQ(callExpr->inputs().size(), 2);
          auto keyExpr = callExpr->inputs()[1].get();
          // TODO: Handle non-constant key expression
          if (auto* constantExpr =
                  dynamic_cast<const core::ConstantTypedExpr*>(keyExpr)) {
            auto keyVariant = constantExpr->value();
            if (keyVariant.hasValue()) {
              std::unique_ptr<common::Filter> keyFilter =
                  makeEqualFilter(callExpr->inputs()[1], evaluator);

              if (keyVariant.kind() == TypeKind::VARCHAR) {
                key = keyVariant.value<TypeKind::VARCHAR>();
                // Use MapSubscriptFilter for map key and value filtering with
                // string key
                filter = common::createMapSubscriptFilter(
                    key, std::move(filter), std::move(keyFilter));
              } else if (keyVariant.kind() == TypeKind::BIGINT) {
                // Handle BIGINT keys
                auto intKey = keyVariant.value<TypeKind::BIGINT>();
                // Use MapSubscriptFilter for map key and value filtering with
                // integer key
                filter = common::createMapSubscriptFilter(
                    intKey, std::move(filter), std::move(keyFilter));
              } else if (keyVariant.kind() == TypeKind::INTEGER) {
                // Handle INTEGER keys
                auto intKey = keyVariant.value<TypeKind::INTEGER>();
                // Use MapSubscriptFilter for map key and value filtering with
                // integer key
                filter = common::createMapSubscriptFilter(
                    intKey, std::move(filter), std::move(keyFilter));
              }
            }
          }
        }

      } else {
        // Fallback or unsupported type for subscript
        return SubfieldFilterResult::unsupported();
      }
    } else {
      return SubfieldFilterResult::unsupported();
    }
  }

  return SubfieldFilterResult::create(subfield, std::move(filter));
}

SubfieldFilterResult ColumnFilterBuilder::buildBetweenFilter(
    const core::CallTypedExpr& expr,
    const common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (expr.inputs().size() < 3) {
    return SubfieldFilterResult::unsupported();
  }

  auto betweenFilter =
      makeBetweenFilter(expr.inputs()[1], expr.inputs()[2], evaluator, negated);
  if (!betweenFilter) {
    return SubfieldFilterResult::unsupported();
  }

  // Check if input is a Function
  const auto& fieldExpr = expr.inputs()[0];
  if (auto functionInfo = FunctionInfo::tryExtractFromExpr(fieldExpr)) {
    std::unique_ptr<common::Filter> filter;
    if (functionInfo->functionName == "cast" && functionInfo->fieldDepth == 1) {
      filter = common::createCastFilter(
          functionInfo->sourceType,
          functionInfo->targetType,
          std::move(betweenFilter));
      return SubfieldFilterResult::create(subfield, std::move(filter));
    } else {
      return SubfieldFilterResult::unsupported();
    }
  }

  return SubfieldFilterResult::create(subfield, std::move(betweenFilter));
}

SubfieldFilterResult ColumnFilterBuilder::buildIsNullFilter(
    const core::CallTypedExpr& expr,
    const common::Subfield& subfield,
    core::ExpressionEvaluator* /* evaluator */,
    bool negated) {
  auto nullFilter = negated ? std::unique_ptr<common::Filter>(isNotNull())
                            : std::unique_ptr<common::Filter>(isNull());

  // Check if input is a Function
  const auto& fieldExpr = expr.inputs()[0];
  if (auto functionInfo = FunctionInfo::tryExtractFromExpr(fieldExpr)) {
    return SubfieldFilterResult::unsupported();
  }

  return SubfieldFilterResult::create(subfield, std::move(nullFilter));
}

SubfieldFilterResult ColumnFilterBuilder::buildInFilter(
    const core::CallTypedExpr& expr,
    const common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (expr.inputs().size() < 2) {
    return SubfieldFilterResult::unsupported();
  }

  // Create IN filter
  auto inFilter = makeInFilter(expr.inputs()[1], evaluator, negated);
  if (!inFilter) {
    return SubfieldFilterResult::unsupported();
  }

  // Check if input is a function call.
  const auto& functionExpr = expr.inputs()[0];
  if (auto functionInfo = FunctionInfo::tryExtractFromExpr(functionExpr)) {
    std::unique_ptr<common::Filter> filter;
    if (functionInfo->functionName == "cast" && functionInfo->fieldDepth == 1) {
      filter = common::createCastFilter(
          functionInfo->sourceType,
          functionInfo->targetType,
          std::move(inFilter));
      return SubfieldFilterResult::create(subfield, std::move(filter));
    } else {
      return SubfieldFilterResult::unsupported();
    }
  }

  return SubfieldFilterResult::create(subfield, std::move(inFilter));
}

SubfieldFilterResult ColumnFilterBuilder::buildLikeFilter(
    const core::CallTypedExpr& expr,
    const common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool /* negated */) {
  if (expr.inputs().size() < 2 ||
      expr.inputs()[1]->typedExprKind() != core::TypedExprKind::kConstant) {
    return SubfieldFilterResult::unsupported();
  }

  auto value = toConstant(expr.inputs()[1], evaluator);
  BOLT_CHECK_EQ(value->typeKind(), TypeKind::VARCHAR);
  auto filter = like(singleValue<StringView>(value));
  return SubfieldFilterResult::create(subfield, std::move(filter));
}

const std::unordered_map<std::string, ColumnFilterBuilder::FilterBuilderFunc>
    ColumnFilterBuilder::filterBuilders_ = {
        {"eq", ColumnFilterBuilder::buildComparisonFilter},
        {"equalto", ColumnFilterBuilder::buildComparisonFilter},
        {"neq", ColumnFilterBuilder::buildComparisonFilter},
        {"notequalto", ColumnFilterBuilder::buildComparisonFilter},
        {"lt", ColumnFilterBuilder::buildComparisonFilter},
        {"lessthan", ColumnFilterBuilder::buildComparisonFilter},
        {"gt", ColumnFilterBuilder::buildComparisonFilter},
        {"greaterthan", ColumnFilterBuilder::buildComparisonFilter},
        {"lte", ColumnFilterBuilder::buildComparisonFilter},
        {"lessthanorequal", ColumnFilterBuilder::buildComparisonFilter},
        {"gte", ColumnFilterBuilder::buildComparisonFilter},
        {"greaterthanorequal", ColumnFilterBuilder::buildComparisonFilter},
        {"between", ColumnFilterBuilder::buildBetweenFilter},

        // For "presto.default"
        {"presto.default.eq", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.neq", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.lt", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.gt", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.lte", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.gte", ColumnFilterBuilder::buildComparisonFilter},
        {"presto.default.between", ColumnFilterBuilder::buildBetweenFilter},

        {"in", ColumnFilterBuilder::buildInFilter},
        {"is_null", ColumnFilterBuilder::buildIsNullFilter},
        {"isnull", ColumnFilterBuilder::buildIsNullFilter},
};

} // namespace bytedance::bolt::exec
