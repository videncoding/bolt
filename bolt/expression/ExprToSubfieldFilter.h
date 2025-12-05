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

#pragma once

#include <string>
#include "bolt/core/ExpressionEvaluator.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/ITypedExpr.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FilterCreator.h"
namespace bytedance::bolt::exec {

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator*);

/// Convert a leaf call expression (no conjunction like AND/OR) to subfield and
/// filter.  Return nullptr if not supported for pushdown.  This is needed
/// because this conversion is frequently applied when extracting filters from
/// remaining filter in readers.  Frequent throw clutters logs and slows down
/// execution.
std::unique_ptr<common::Filter> leafCallToSubfieldFilter(
    const core::CallTypedExpr&,
    common::Subfield&,
    core::ExpressionEvaluator*,
    bool negated = false);

struct SubfieldFilterResult {
  std::optional<common::Subfield> subfield;
  std::unique_ptr<common::Filter> filter;

  static SubfieldFilterResult unsupported() {
    std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
    path.push_back(std::make_unique<common::Subfield::AllSubscripts>());
    return SubfieldFilterResult{
        std::make_optional<common::Subfield>(std::move(path)), nullptr};
  }

  static SubfieldFilterResult create(
      const common::Subfield& subfield,
      std::unique_ptr<common::Filter>&& filter) {
    return SubfieldFilterResult{
        std::make_optional(subfield.clone()), std::move(filter)};
  }
};

class ColumnFilterBuilder {
 public:
  using FilterBuilderFunc = SubfieldFilterResult (*)(
      const core::CallTypedExpr&,
      const common::Subfield&,
      core::ExpressionEvaluator*,
      bool);

  static SubfieldFilterResult tryBuild(
      const core::CallTypedExpr& expr,
      core::ExpressionEvaluator* evaluator,
      bool negated);

 private:
  static std::optional<common::Subfield> extractColumnAccess(
      const core::ITypedExpr* expr);

  /// Builds a comparison filter for expressions like:
  /// - field <op> constant
  /// - constant <op> field
  /// - CAST(field) <op> constant
  /// - constant <op> CAST(field)
  /// - CAST(field) <op> CAST(constant)
  ///
  /// where <op> can be =, !=, <, <=, >, >= and field may have a CAST
  /// expression.
  ///
  /// Examples of supported expressions:
  /// - CAST(column AS DOUBLE) > 10.5
  /// - column = 42
  /// - 100 < CAST(column AS INTEGER)
  /// - CAST(column AS DOUBLE) = CAST('123' AS DOUBLE)
  ///
  /// @param expr The comparison expression to build a filter for. Must contain
  /// exactly two inputs:
  ///            one side must be a field access (possibly with CAST) and the
  ///            other must be a constant or an expression that can be evaluated
  ///            to a constant. The constant side may also include a CAST.
  /// @param subfield The extracted field path from the expression.
  /// @param evaluator Expression evaluator used to evaluate non-constant
  /// expressions to constants.
  /// @param negated Whether the filter should be negated.
  ///
  /// @return A SubfieldFilterResult containing:
  ///         - A Filter implementing the comparison if successful
  ///         - The subfield representing the column being filtered
  ///         - Empty filter and invalid subfield if the expression is not
  ///         supported
  ///
  /// The function handles:
  /// - Commutative operators (=, !=) and non-commutative operators (<, <=, >,
  /// >=)
  /// - Swapped operands (constant on left vs right)
  /// - Negation of comparisons
  /// - CAST expressions on either or both sides
  /// - Evaluation of non-constant expressions to constants
  ///
  /// The function returns unsupported result if:
  /// - The expression has fewer than 2 inputs
  /// - Neither input contains a field access
  /// - The non-field input cannot be evaluated to a constant
  /// - The comparison operator is not supported
  static SubfieldFilterResult buildComparisonFilter(
      const core::CallTypedExpr& expr,
      const common::Subfield& subfield,
      core::ExpressionEvaluator* evaluator,
      bool negated);

  static SubfieldFilterResult buildBetweenFilter(
      const core::CallTypedExpr& expr,
      const common::Subfield& subfield,
      core::ExpressionEvaluator* evaluator,
      bool negated);

  static SubfieldFilterResult buildInFilter(
      const core::CallTypedExpr& expr,
      const common::Subfield& subfield,
      core::ExpressionEvaluator* evaluator,
      bool negated);

  static SubfieldFilterResult buildIsNullFilter(
      const core::CallTypedExpr& expr,
      const common::Subfield& subfield,
      core::ExpressionEvaluator*,
      bool negated);

  static SubfieldFilterResult buildLikeFilter(
      const core::CallTypedExpr& expr,
      const common::Subfield& subfield,
      core::ExpressionEvaluator* evaluator,
      bool negated);

  static const std::unordered_map<std::string, FilterBuilderFunc>
      filterBuilders_;
};

inline bool isEqualityFn(const std::string& functionName) {
  return functionName == "eq" || functionName == "presto.default.eq" ||
      functionName == "equalto";
}

inline bool isNotEqualFn(const std::string& functionName) {
  return functionName == "neq" || functionName == "presto.default.neq" ||
      functionName == "notequalto";
}

inline bool isLessThanFn(const std::string& functionName) {
  return functionName == "lt" || functionName == "presto.default.lt" ||
      functionName == "lessthan";
}

inline bool isGreaterThanFn(const std::string& functionName) {
  return functionName == "gt" || functionName == "presto.default.gt" ||
      functionName == "greaterthan";
}

inline bool isLessThanOrEqualFn(const std::string& functionName) {
  return functionName == "lte" || functionName == "presto.default.lte" ||
      functionName == "lessthanorequal";
}

inline bool isGreaterThanOrEqualFn(const std::string& functionName) {
  return functionName == "gte" || functionName == "presto.default.gte" ||
      functionName == "greaterthanorequal";
}

inline bool isLikeFn(const std::string& functionName) {
  return functionName == "like" ||
      functionName ==
      "presto.default.like"; // Note: no presto.default alias for 'like'
}

} // namespace bytedance::bolt::exec
