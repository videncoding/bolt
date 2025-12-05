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

#include "bolt/expression/ExprToSubfieldFilter.h"
#include <gtest/gtest.h>
#include "bolt/expression/Expr.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/TypeResolver.h"
namespace bytedance::bolt::exec {
namespace {
using namespace bytedance::bolt::common;

void validateSubfield(
    const Subfield& subfield,
    const std::vector<std::string>& expectedPath) {
  ASSERT_EQ(subfield.path().size(), expectedPath.size());
  for (int i = 0; i < expectedPath.size(); ++i) {
    ASSERT_TRUE(subfield.path()[i]);
    ASSERT_EQ(*subfield.path()[i], Subfield::NestedField(expectedPath[i]));
  }
}

class ExprToSubfieldFilterTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  core::TypedExprPtr parseExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    return core::Expressions::inferTypes(
        parse::parseExpr(expr, {}), type, pool_.get());
  }

  core::CallTypedExprPtr parseCallExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        parseExpr(expr, type));
    BOLT_CHECK_NOT_NULL(call);
    return call;
  }

  core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  SimpleExpressionEvaluator evaluator_{queryCtx_.get(), pool_.get()};
};

TEST_F(ExprToSubfieldFilterTest, eq) {
  auto call = parseCallExpr("a = 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, eqExpr) {
  auto call = parseCallExpr("a = 21 * 2", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, eqSubfield) {
  auto call = parseCallExpr("a.b = 42", ROW({{"a", ROW({{"b", BIGINT()}})}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a", "b"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, neq) {
  auto call = parseCallExpr("a <> 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, lte) {
  auto call = parseCallExpr("a <= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, lt) {
  auto call = parseCallExpr("a < 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gte) {
  auto call = parseCallExpr("a >= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gt) {
  auto call = parseCallExpr("a > 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, between) {
  auto call = parseCallExpr("a between 40 and 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), 40 <= i && i <= 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, in) {
  auto call = parseCallExpr("a in (40, 42)", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), i == 40 || i == 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, isNull) {
  auto call = parseCallExpr("a is null", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(0));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, isNotNull) {
  auto call = parseCallExpr("a is not null", ROW({{"a", BIGINT()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(0));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, like) {
  auto call = parseCallExpr("a like 'foo%'", ROW({{"a", VARCHAR()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, nonConstant) {
  auto call =
      parseCallExpr("a = b + 1", ROW({{"a", BIGINT()}, {"b", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, userError) {
  auto call = parseCallExpr("a = 1 / 0", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, dereferenceWithEmptyField) {
  auto call = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::DereferenceTypedExpr>(
              REAL(),
              std::make_shared<core::FieldAccessTypedExpr>(
                  ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}}),
                  std::make_shared<core::InputTypedExpr>(ROW(
                      {{"c0",
                        ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}})}})),
                  "c0"),
              1)},
      "is_null");
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}
TEST_F(ExprToSubfieldFilterTest, orFloatRanges) {
  // Test OR between two float ranges
  auto call = parseCallExpr("a > 1.5 OR a < -2.5", ROW({{"a", REAL()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values
  ASSERT_FALSE(filter->testFloat(-1.0f)); // Between -2.5 and 1.5
  ASSERT_TRUE(filter->testFloat(-3.0f)); // Less than -2.5
  ASSERT_TRUE(filter->testFloat(2.0f)); // Greater than 1.5
  ASSERT_FALSE(filter->testFloat(0.0f)); // Between -2.5 and 1.5
  ASSERT_FALSE(filter->testNull()); // Nulls not allowed by default
}

TEST_F(ExprToSubfieldFilterTest, orDoubleRanges) {
  // Test OR between two double ranges
  auto call =
      parseCallExpr("a >= 100.25 OR a <= -50.75", ROW({{"a", DOUBLE()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values
  ASSERT_FALSE(filter->testDouble(0.0)); // Between -50.75 and 100.25
  ASSERT_TRUE(filter->testDouble(-51.0)); // Less than or equal to -50.75
  ASSERT_TRUE(filter->testDouble(101.0)); // Greater than or equal to 100.25
  ASSERT_TRUE(filter->testDouble(-50.75)); // Equal to lower bound
  ASSERT_TRUE(filter->testDouble(100.25)); // Equal to upper bound
  ASSERT_FALSE(filter->testDouble(50.0)); // Between bounds
  ASSERT_FALSE(filter->testNull()); // Nulls not allowed by default
}

TEST_F(ExprToSubfieldFilterTest, orMultipleFloatRanges) {
  // Test OR between multiple float ranges
  auto call = parseCallExpr(
      "a between 1.0 and 2.0 OR a between 4.0 and 5.0 OR a between 7.0 and 8.0",
      ROW({{"a", REAL()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values
  ASSERT_TRUE(filter->testFloat(1.5f)); // In first range
  ASSERT_TRUE(filter->testFloat(4.5f)); // In second range
  ASSERT_TRUE(filter->testFloat(7.5f)); // In third range
  ASSERT_FALSE(filter->testFloat(3.0f)); // Between first and second range
  ASSERT_FALSE(filter->testFloat(6.0f)); // Between second and third range
  ASSERT_FALSE(filter->testFloat(9.0f)); // After last range
  ASSERT_FALSE(filter->testNull()); // Nulls not allowed
}

TEST_F(ExprToSubfieldFilterTest, orMultipleDoubleRanges) {
  // Test complex condition with multiple ranges
  auto call = parseCallExpr(
      "a between -1.5 and 1.5 OR a between 3.5 and 5.5 OR a > 8.5",
      ROW({{"a", DOUBLE()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values
  ASSERT_TRUE(filter->testDouble(0.0)); // In first range
  ASSERT_TRUE(filter->testDouble(4.5)); // In second range
  ASSERT_TRUE(filter->testDouble(9.0)); // In third range (> 8.5)
  ASSERT_FALSE(filter->testDouble(2.5)); // Between first and second range
  ASSERT_FALSE(filter->testDouble(6.5)); // Between second and third range
  ASSERT_FALSE(filter->testDouble(-2.0)); // Before first range
  ASSERT_FALSE(filter->testNull()); // Nulls not allowed
}

TEST_F(ExprToSubfieldFilterTest, orFloatWithNulls) {
  // Test OR operation with IS NULL condition
  auto call = parseCallExpr("a > 5.5 OR a IS NULL", ROW({{"a", REAL()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values
  ASSERT_FALSE(filter->testFloat(5.0f)); // Less than bound
  ASSERT_TRUE(filter->testFloat(6.0f)); // Greater than bound
  ASSERT_TRUE(filter->testNull()); // Nulls allowed
}

TEST_F(ExprToSubfieldFilterTest, orDoubleWithNaN) {
  // Test handling of edge cases including NaN
  auto call = parseCallExpr("a > 1.0 OR a < -1.0", ROW({{"a", DOUBLE()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});

  // Test values including NaN
  ASSERT_TRUE(filter->testDouble(2.0)); // Greater than upper bound
  ASSERT_TRUE(filter->testDouble(-2.0)); // Less than lower bound
  ASSERT_FALSE(filter->testDouble(0.0)); // Between bounds
  ASSERT_FALSE(filter->testDouble(
      std::numeric_limits<double>::quiet_NaN())); // NaN should not pass
}
} // namespace
} // namespace bytedance::bolt::exec
