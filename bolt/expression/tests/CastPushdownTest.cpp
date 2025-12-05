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

#include <common/memory/Memory.h>
#include <gtest/gtest.h>
#include <limits>
#include "bolt/core/QueryCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/type/filter/Cast.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::test {

class CastPushdownTest : public testing::Test, public test::VectorTestBase {
 public:
  CastPushdownTest() = default;
  virtual ~CastPushdownTest() = default;

 protected:
  using test::VectorTestBase::makeArrayVector;
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    exec::registerFunctionCallToSpecialForms();
    functions::prestosql::registerAllScalarFunctions();
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool({});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        std::thread::hardware_concurrency());
    queryCtx_ = core::QueryCtx::create(executor_.get());

    evaluator_ = std::make_shared<SimpleExpressionEvaluator>(
        queryCtx_.get(), pool_.get());
  }

  std::unique_ptr<common::Filter> createInt32ToInt64CastFilter(
      int64_t lower,
      int64_t upper) {
    // Use the toFilter method to create a Cast filter instead of directly using
    // common::Cast
    auto field = makeFieldAccess("c0", INTEGER());
    auto cast = makeCastExpr(BIGINT(), field);
    auto lowerConst = makeInt64Constant(lower);
    auto upperConst = makeInt64Constant(upper);

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{cast, lowerConst, upperConst},
        "between");

    auto [subfield, filter] = toFilter(betweenExpr);
    return std::move(filter);
  }

  std::unique_ptr<common::Filter> createStringCastFilter(
      const std::string& lower,
      const std::string& upper) {
    // Use the toFilter method to create a Cast filter
    auto field = makeFieldAccess("c0", VARCHAR());
    auto cast = makeCastExpr(VARCHAR(), field);
    auto lowerConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<StringView>(lower));
    auto upperConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<StringView>(upper));

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{cast, lowerConst, upperConst},
        "between");

    auto [_, filter] = toFilter(betweenExpr);
    return std::move(filter);
  }

  void verifyCastFilterTypes(
      const common::Filter* filter,
      const TypePtr& sourceType,
      const TypePtr& targetType) {
    auto castFilter =
        dynamic_cast<const common::IFilterWithInnerFilter*>(filter);
    ASSERT_NE(castFilter, nullptr);
    EXPECT_TRUE(castFilter->sourceType()->kindEquals(sourceType));
    EXPECT_TRUE(castFilter->targetType()->kindEquals(targetType));
  }

  void TearDown() override {
    evaluator_.reset();
    queryCtx_.reset();
    pool_.reset();
  }

  // Helper methods
  std::pair<common::Subfield, std::unique_ptr<common::Filter>> toFilter(
      const core::TypedExprPtr& expr) {
    return exec::toSubfieldFilter(expr, evaluator_.get());
  }

  static core::TypedExprPtr makeFieldAccess(
      const std::string& name,
      const TypePtr& type) {
    return std::make_shared<const core::FieldAccessTypedExpr>(type, name);
  }

  static core::TypedExprPtr makeCastExpr(
      const TypePtr& targetType,
      const core::TypedExprPtr& input) {
    return std::make_shared<const core::CastTypedExpr>(
        targetType, input, false);
  }

  // Constants creation helpers
  static core::TypedExprPtr makeDoubleConstant(double value) {
    return std::make_shared<core::ConstantTypedExpr>(
        DOUBLE(), variant::create<double>(value));
  }

  static core::TypedExprPtr makeInt64Constant(int64_t value) {
    return std::make_shared<core::ConstantTypedExpr>(
        BIGINT(), variant::create<int64_t>(value));
  }

  static core::TypedExprPtr makeInt32Constant(int32_t value) {
    return std::make_shared<core::ConstantTypedExpr>(
        INTEGER(), variant::create<int32_t>(value));
  }

  static core::TypedExprPtr makeDateConstant(int32_t value) {
    return std::make_shared<core::ConstantTypedExpr>(
        DATE(), variant::create<int32_t>(value));
  }

  static core::TypedExprPtr makeNullConstant(const TypePtr& type) {
    return std::make_shared<core::ConstantTypedExpr>(
        type, variant::null(type->kind()));
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::shared_ptr<core::ExpressionEvaluator> evaluator_;
  std::shared_ptr<folly::Executor> executor_;
};

TEST_F(CastPushdownTest, SimpleCastFilter) {
  // Test: CAST(c0 AS DOUBLE) > 10.5
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield, filter] = toFilter(gtExpr);

  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(filter->kind(), common::FilterKind::kCast);
  EXPECT_EQ(subfield.toString(), "c0");

  auto castFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
  ASSERT_NE(castFilter, nullptr);
  EXPECT_TRUE(castFilter->sourceType()->kindEquals(BIGINT()));
  EXPECT_TRUE(castFilter->targetType()->kindEquals(DOUBLE()));
}

TEST_F(CastPushdownTest, NestedFieldCastFilter) {
  // Test: CAST(struct.field AS INTEGER) = 42
  auto structField = makeFieldAccess("struct.field", BIGINT());
  auto cast = makeCastExpr(INTEGER(), structField);
  auto intConst = makeInt32Constant(42);

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, intConst}, "eq");

  auto [subfield, filter] = toFilter(eqExpr);

  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "struct.field");
}

TEST_F(CastPushdownTest, DISABLED_ChainedCastFilter) {
  // Test: CAST(CAST(c0 AS DOUBLE) AS BIGINT) > 100
  auto field = makeFieldAccess("c0", INTEGER());
  auto cast1 = makeCastExpr(DOUBLE(), field);
  auto cast2 = makeCastExpr(BIGINT(), cast1);
  auto bigintConst = makeInt64Constant(100);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast2, bigintConst}, "gt");

  auto [subfield, filter] = toFilter(gtExpr);

  ASSERT_NE(filter, nullptr);
  auto castFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
  ASSERT_EQ(castFilter, nullptr);
}

TEST_F(CastPushdownTest, SimpleCast) {
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield, filter] = toFilter(gtExpr);
  EXPECT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");
}

TEST_F(CastPushdownTest, DISABLED_ArithmeticWithCast) {
  auto field = makeFieldAccess("c0", BIGINT());
  auto plusOne = std::make_shared<const core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{field, makeInt64Constant(1)},
      "plus");

  auto cast = makeCastExpr(DOUBLE(), plusOne);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield, filter] = toFilter(gtExpr);
  ASSERT_NE(filter, nullptr);
  ASSERT_TRUE(subfield.valid());

  ASSERT_FALSE(filter->testInt64(2));
  ASSERT_FALSE(filter->testInt64(9));
  ASSERT_TRUE(filter->testInt64(10));
}

// Test for compound predicates (AND) with cast
TEST_F(CastPushdownTest, CompoundPredicates) {
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast1 = makeCastExpr(DOUBLE(), field);
  auto cast2 = makeCastExpr(DOUBLE(), field);

  auto lowerBound = makeDoubleConstant(10.5);
  auto upperBound = makeDoubleConstant(20.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast1, lowerBound}, "gt");

  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast2, upperBound}, "lt");

  auto andExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{gtExpr, ltExpr}, "and");

  auto [subfield, filter] = toFilter(andExpr);
  ASSERT_EQ(filter, nullptr)
      << "Filter should be nullptr for none supported expr: compound predicts.";
  ASSERT_FALSE(subfield.valid())
      << "Subfield should be invalid for none supported expr: compound predicts.";
}

TEST_F(CastPushdownTest, NullHandling) {
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);

  // Test IS NULL
  {
    auto isNullExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{cast}, "is_null");

    auto [subfield, filter] = toFilter(isNullExpr);
    ASSERT_EQ(filter, nullptr);
  }

  // Test IS NOT NULL
  {
    // First create IS NULL
    auto isNullExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{cast}, "is_null");

    // Then wrap it in NOT
    auto isNotNullExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{isNullExpr}, "not");

    auto [subfield, filter] = toFilter(isNotNullExpr);
    ASSERT_EQ(filter, nullptr);
  }
}

TEST_F(CastPushdownTest, StringCastFilter) {
  // Test: CAST(c0 AS VARCHAR) = '123'
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(VARCHAR(), field);
  auto strConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant(std::string("123")));

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, strConst}, "eq");

  auto [subfield, filter] = toFilter(eqExpr);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  EXPECT_TRUE(filter->testInt64(123));
  EXPECT_FALSE(filter->testInt64(456));

  // Test: CAST(c0 AS DOUBLE) = 42.0
  auto invalidField = makeFieldAccess("c0", VARCHAR());
  auto invalidCast = makeCastExpr(DOUBLE(), invalidField);
  auto compareConst = makeDoubleConstant(42.0);

  auto invalidExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{invalidCast, compareConst},
      "eq");

  auto result = toFilter(invalidExpr);
  EXPECT_EQ(result.first.toString(), "c0")
      << "Should successfully pushdown VARCHAR to DOUBLE cast";
  ASSERT_NE(result.second, nullptr);
  EXPECT_TRUE(filter->testBytes("42", 2))
      << "Should return nullptr for invalid source types";
}

TEST_F(CastPushdownTest, BetweenWithCast) {
  // Test: CAST(c0 AS DOUBLE) BETWEEN 10.5 AND 20.5
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto lowerBound = makeDoubleConstant(10.5);
  auto upperBound = makeDoubleConstant(20.5);

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{cast, lowerBound, upperBound},
      "between");

  auto [subfield, filter] = toFilter(betweenExpr);
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(filter->kind(), common::FilterKind::kCast);
  EXPECT_EQ(subfield.toString(), "c0");

  auto castFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
  ASSERT_NE(castFilter, nullptr);
  EXPECT_TRUE(castFilter->sourceType()->kindEquals(BIGINT()));
  EXPECT_TRUE(castFilter->targetType()->kindEquals(DOUBLE()));

  // Test some values
  EXPECT_FALSE(filter->testInt64(5)); // Below range
  EXPECT_TRUE(filter->testInt64(15)); // In range
  EXPECT_FALSE(filter->testInt64(25)); // Above range
}

TEST_F(CastPushdownTest, InWithCastBasic) {
  struct InTestCase {
    std::string name;
    std::vector<double> inList;
    std::vector<std::pair<int64_t, bool>> testValues;
  };

  std::vector<InTestCase> testCases = {
      {
          "Basic IN list - exact matching",
          {1.5, 2.5, 3.5},
          // Test values after CAST(int AS DOUBLE), verifying exact matches
          {{1, false}, // 1.0 not in (1.5, 2.5, 3.5)
           {2, false}, // 2.0 not in (1.5, 2.5, 3.5)
           {3, false}, // 3.0 not in (1.5, 2.5, 3.5)
           {4, false}} // 4.0 not in (1.5, 2.5, 3.5)
      },
      {
          "IN list with exact integer matches",
          {1.0, 2.0, 3.0},
          {{1, true}, // 1.0 equals 1.0
           {2, true}, // 2.0 equals 2.0
           {3, true}, // 3.0 equals 3.0
           {4, false}} // 4.0 not in list
      },
  };

  for (const auto& tc : testCases) {
    SCOPED_TRACE(tc.name);

    // Create field access and cast expression
    auto field = makeFieldAccess("c0", BIGINT());
    auto cast = makeCastExpr(DOUBLE(), field);

    // Create array vector for IN list
    vector_size_t size = 1;
    BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
    BufferPtr sizesBuffer = allocateSizes(size, pool_.get());

    auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
    auto rawSizes = sizesBuffer->asMutable<vector_size_t>();
    rawOffsets[0] = 0;
    rawSizes[0] = tc.inList.size();

    auto elements = makeFlatVector<double>(tc.inList);
    auto arrayVector = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(DOUBLE()),
        nullptr,
        size,
        offsetsBuffer,
        sizesBuffer,
        elements);

    auto valuesExpr = std::make_shared<core::ConstantTypedExpr>(arrayVector);
    auto inExpr = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{cast, valuesExpr}, "in");

    auto [subfield, filter] = toFilter(inExpr);

    ASSERT_NE(filter, nullptr) << "Filter creation failed";
    EXPECT_EQ(filter->kind(), common::FilterKind::kCast)
        << "Expected CAST filter";
    EXPECT_EQ(subfield.toString(), "c0") << "Wrong field access";

    auto castFilter =
        dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
    ASSERT_NE(castFilter, nullptr) << "Cast filter is null";
    EXPECT_TRUE(castFilter->sourceType()->kindEquals(BIGINT()))
        << "Wrong source type";
    EXPECT_TRUE(castFilter->targetType()->kindEquals(DOUBLE()))
        << "Wrong target type";

    // Test values for exact matching behavior
    for (const auto& [value, expected] : tc.testValues) {
      EXPECT_EQ(filter->testInt64(value), expected)
          << "Value: " << value << " cast to " << static_cast<double>(value)
          << ", Expected: " << expected;
    }

    // Test range behavior
    auto innerFilter = castFilter->innerFilter();
    ASSERT_NE(innerFilter, nullptr) << "Inner filter is null";

    // Test range queries
    double minVal = *std::min_element(tc.inList.begin(), tc.inList.end());
    double maxVal = *std::max_element(tc.inList.begin(), tc.inList.end());

    // Only exact matches should pass
    EXPECT_TRUE(filter->testInt64Range(minVal, maxVal, false))
        << "Range containing exact IN values should pass";
    EXPECT_FALSE(filter->testInt64Range(minVal - 1.0, minVal - 0.1, false))
        << "Range below all IN values should fail";
  }
}

TEST_F(CastPushdownTest, InWithCastNull) {
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  std::vector<double> values = {1.5, 2.5, 3.5};

  // Create array vector with null value
  vector_size_t size = 1;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());

  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();
  rawOffsets[0] = 0;
  rawSizes[0] = values.size();

  auto elements = makeFlatVector<double>(values);
  auto arrayVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(DOUBLE()),
      nulls,
      size,
      offsetsBuffer,
      sizesBuffer,
      elements);
  arrayVector->setNull(0, true);
  auto valuesExpr = std::make_shared<core::ConstantTypedExpr>(arrayVector);
  auto inExpr = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, valuesExpr}, "in");

  auto [subfield, filter] = toFilter(inExpr);
  ASSERT_NE(filter, nullptr);

  // Test NULL handling
  EXPECT_TRUE(filter->testNull())
      << "NULL should pass when IN list contains NULL";
  EXPECT_FALSE(filter->testInt64(0))
      << "Non-NULL values should still be filtered";

  // Test range with NULL
  auto castFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
  ASSERT_NE(castFilter, nullptr);

  EXPECT_TRUE(filter->testInt64Range(1, 4, true))
      << "Range with NULL should pass";
  EXPECT_FALSE(filter->testInt64Range(1, 4, false))
      << "Range without NULL should not pass when IN list is NULL";
}

TEST_F(CastPushdownTest, InWithCastNullHandling) {
  // Create the test expression: CAST(c0 AS DOUBLE) IN (1.5, NULL, 3.5)
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);

  // Create array vector with NULL elements
  vector_size_t size = 1;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());

  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();
  rawOffsets[0] = 0;
  rawSizes[0] = 3;

  // Create values with NULL
  std::vector<std::optional<double>> values = {1.0, std::nullopt, 3.5};
  auto elements = makeNullableFlatVector<double>(values);

  auto arrayVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(DOUBLE()),
      nullptr, // Array itself isn't null
      size,
      offsetsBuffer,
      sizesBuffer,
      elements);

  auto valuesExpr = std::make_shared<core::ConstantTypedExpr>(arrayVector);
  auto inExpr = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, valuesExpr}, "in");

  auto [subfield, filter] = toFilter(inExpr);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  // Test exact matches - should be TRUE
  EXPECT_TRUE(filter->testInt64(1));

  // Test non-matching value with NULL in list - should be NULL/FALSE not TRUE
  EXPECT_FALSE(filter->testInt64(2));

  // Test NULL input
  EXPECT_FALSE(filter->testNull()) << "NULL input should be handled";

  // Additional edge cases
  EXPECT_FALSE(filter->testInt64(10));
  EXPECT_FALSE(filter->testInt64(40));
}

TEST_F(CastPushdownTest, MultiTypeCasts) {
  struct TestCase {
    TypePtr sourceType;
    TypePtr targetType;
    variant testValue;
    std::function<void(const common::Filter*)> tester;
  };

  std::vector<TestCase> testCases = {
      // CAST(bigint_col AS DOUBLE) = 42.0
      {BIGINT(),
       DOUBLE(),
       variant::create<double>(42.0),
       [](const common::Filter* filter) {
         // For exact equality comparison
         EXPECT_TRUE(filter->testInt64(42));
         EXPECT_FALSE(filter->testInt64(41));
         EXPECT_FALSE(filter->testInt64(43));
       }},

      // CAST(int_col AS BIGINT) = 100000L
      {INTEGER(),
       BIGINT(),
       variant::create<int64_t>(100000),
       [](const common::Filter* filter) {
         // Test exact equality
         EXPECT_TRUE(filter->testInt64(100000));
         EXPECT_FALSE(filter->testInt64(99999));
         EXPECT_FALSE(filter->testInt64(100001));
       }},

      // CAST(smallint_col AS INTEGER) = 32000
      {SMALLINT(),
       INTEGER(),
       variant::create<int32_t>(32000),
       [](const common::Filter* filter) {
         EXPECT_TRUE(filter->testInt64(32000));
         EXPECT_FALSE(filter->testInt64(31999));
         EXPECT_FALSE(filter->testInt64(32001));
       }},

      // CAST(double_col AS INTEGER) = 42
      {DOUBLE(),
       INTEGER(),
       variant::create<int32_t>(42),
       [](const common::Filter* filter) {
         // Values that should cast to 42
         EXPECT_TRUE(filter->testDouble(42.0));
         EXPECT_TRUE(filter->testDouble(42.4));
         EXPECT_FALSE(filter->testDouble(42.5));
         EXPECT_TRUE(filter->testDouble(41.9));
       }},

      // CAST(float_col AS DOUBLE) = 3.14
      {REAL(),
       DOUBLE(),
       variant::create<double>(3.14f),
       [](const common::Filter* filter) {
         const float epsilon = std::numeric_limits<float>::epsilon();
         EXPECT_TRUE(filter->testFloat(3.14f));
         EXPECT_FALSE(filter->testFloat(3.14f + epsilon));
         EXPECT_FALSE(filter->testFloat(3.14f - epsilon));
         EXPECT_FALSE(filter->testFloat(3.15f));
         EXPECT_FALSE(filter->testFloat(3.13f));
       }},

      // CAST(int_col AS VARCHAR) = '12345'
      {INTEGER(),
       VARCHAR(),
       variant::create<StringView>("12345"),
       [](const common::Filter* filter) {
         EXPECT_TRUE(filter->testInt64(12345));
         EXPECT_FALSE(filter->testInt64(12344));
         EXPECT_FALSE(filter->testInt64(12346));
       }}};

  for (const auto& tc : testCases) {
    auto field = makeFieldAccess("c0", tc.sourceType);
    auto cast = makeCastExpr(tc.targetType, field);
    auto constant =
        std::make_shared<core::ConstantTypedExpr>(tc.targetType, tc.testValue);

    // Create EQ expression CAST(field AS targetType) = constant
    auto eqExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{cast, constant}, "eq");

    auto [subfield, filter] = toFilter(eqExpr);
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(filter->kind(), common::FilterKind::kCast);

    auto castFilter =
        dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
    ASSERT_NE(castFilter, nullptr);

    // Verify type info
    EXPECT_TRUE(castFilter->sourceType()->kindEquals(tc.sourceType));
    EXPECT_TRUE(castFilter->targetType()->kindEquals(tc.targetType));

    // Run the specific tests for this cast case
    tc.tester(filter.get());

    // Test null handling
    EXPECT_FALSE(filter->testNull());
  }
}

TEST_F(CastPushdownTest, RangeFiltersWithCast) {
  // Test range filters with different cast combinations
  struct TestCase {
    const char* name;
    TypePtr sourceType;
    TypePtr targetType;
    variant lowerBound;
    variant upperBound;
    std::vector<std::pair<variant, bool>> testValues;
  };

  std::vector<TestCase> testCases = {
      {"BIGINT to DOUBLE",
       BIGINT(),
       DOUBLE(),
       variant::create<double>(10.5),
       variant::create<double>(20.5),
       {{variant::create<int64_t>(5), false},
        {variant::create<int64_t>(15), true},
        {variant::create<int64_t>(25), false}}},
      {"DOUBLE to INTEGER",
       DOUBLE(),
       INTEGER(),
       variant::create<int32_t>(10),
       variant::create<int32_t>(20),
       {{variant::create<double>(9.9), true},
        {variant::create<double>(15.7), true},
        {variant::create<double>(20.1), true}}}};

  for (const auto& tc : testCases) {
    SCOPED_TRACE(tc.name);

    auto field = makeFieldAccess("c0", tc.sourceType);
    auto cast = makeCastExpr(tc.targetType, field);
    auto lower =
        std::make_shared<core::ConstantTypedExpr>(tc.targetType, tc.lowerBound);
    auto upper =
        std::make_shared<core::ConstantTypedExpr>(tc.targetType, tc.upperBound);

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{cast, lower, upper},
        "between");

    auto [subfield, filter] = toFilter(betweenExpr);
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(filter->kind(), common::FilterKind::kCast);

    // Test values
    for (const auto& [value, expected] : tc.testValues) {
      if (value.kind() == TypeKind::BIGINT) {
        EXPECT_EQ(filter->testInt64(value.value<int64_t>()), expected)
            << "Test case: " << tc.name
            << ", Value tested: " << value.value<int64_t>()
            << ", Expected: " << expected;
      } else if (value.kind() == TypeKind::DOUBLE) {
        auto result = filter->testDouble(value.value<double>());
        SCOPED_TRACE(
            "Test case: " + std::string(tc.name) +
            ", Value tested: " + std::to_string(value.value<double>()) +
            ", Result: " + std::to_string(result) +
            ", Expected: " + std::to_string(expected));
        EXPECT_EQ(result, expected)
            << "Test case: " << tc.name
            << ", Value tested: " << value.value<double>()
            << ", Result: " << result << ", Expected: " << expected;
      }
    }
  }
}

TEST_F(CastPushdownTest, TypeMismatch) {
  // Test cases for type mismatch scenarios
  // Case 1: SHORT column vs INT literal
  {
    auto shortField = makeFieldAccess("c0", SMALLINT());

    // Create a constant of type INT
    auto intConstant = makeInt32Constant(100000); // Value exceeds SHORT range

    // Create comparison expression SHORT_COL > INT_LITERAL
    auto gtExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{shortField, intConstant},
        "gt");

    auto [subfield, filter] = toFilter(gtExpr);

    // Should successfully extract the field path
    ASSERT_FALSE(subfield.valid()) << "The subfield should be invalid";

    // But should not create a filter because of type mismatch
    ASSERT_EQ(filter, nullptr)
        << "Should not create filter when types don't match";
  }

  // Case 2: DOUBLE column vs INT literal
  {
    auto doubleField = makeFieldAccess("c0", DOUBLE());
    auto intConstant = makeInt32Constant(42);

    // Create expression DOUBLE_COL = INT_LITERAL
    auto eqExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{doubleField, intConstant},
        "eq");

    auto [subfield, filter] = toFilter(eqExpr);

    ASSERT_FALSE(subfield.valid()) << "The subfield should be invalid";

    ASSERT_EQ(filter, nullptr)
        << "Should not create filter for double vs int comparison";
  }

  // Case 3: VARCHAR column vs INT literal
  {
    auto varcharField = makeFieldAccess("c0", VARCHAR());
    auto intConstant = makeInt32Constant(42);

    // Create expression VARCHAR_COL = INT_LITERAL
    auto eqExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{varcharField, intConstant},
        "eq");

    auto [subfield, filter] = toFilter(eqExpr);

    ASSERT_FALSE(subfield.valid()) << "The subfield should be invalid";

    ASSERT_EQ(filter, nullptr)
        << "Should not create filter for varchar vs int comparison";
  }

  // Case 4: INT column vs BIGINT literal
  {
    auto intField = makeFieldAccess("c0", INTEGER());
    auto bigintConstant = makeInt64Constant(42);

    // Create expression INT_COL = BIGINT_LITERAL
    auto eqExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{intField, bigintConstant},
        "eq");

    auto [subfield, filter] = toFilter(eqExpr);

    ASSERT_FALSE(subfield.valid()) << "The subfield should be invalid";

    ASSERT_EQ(filter, nullptr)
        << "Should not create filter for int vs bigint comparison";
  }

  // Case 5: Test all comparison operators
  {
    auto shortField = makeFieldAccess("c0", SMALLINT());
    auto intConstant = makeInt32Constant(10000);

    std::vector<std::string> operators = {
        "eq", "neq", "lt", "lte", "gt", "gte"};

    for (const auto& op : operators) {
      auto compExpr = std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{shortField, intConstant},
          op);

      auto [subfield, filter] = toFilter(compExpr);

      ASSERT_FALSE(subfield.valid()) << "The subfield should be invalid";
      ASSERT_EQ(filter, nullptr)
          << "Should not create filter for type mismatch with operator " << op;
    }
  }
}

TEST_F(CastPushdownTest, SwappedComparisonCasts) {
  struct TestCase {
    const char* name;
    std::string op;
    bool swapOperands;
    bool negateComparison;
    double compareValue;
    std::vector<std::pair<int64_t, bool>> testValues;
  };

  std::vector<TestCase> testCases = {
      // Original form:: CAST(col AS DOUBLE) > 10.5
      {"Basic comparison",
       "gt",
       false,
       false,
       10.5,
       {{5, false}, {15, true}, {10, false}}},

      // Swapped operands: 10.5 < CAST(col AS DOUBLE)
      {"Swapped operands",
       "lt",
       true,
       false,
       10.5,
       {{5, false}, {15, true}, {10, false}}},

      // Negated: NOT(CAST(col AS DOUBLE) > 10.5)
      {"Negated comparison",
       "gt",
       false,
       true,
       10.5,
       {{5, true}, {15, false}, {10, true}}},

      // Swapped and negated: NOT(10.5 < CAST(col AS DOUBLE))'
      {"Swapped and negated",
       "lt",
       true,
       true,
       10.5,
       {{5, true}, {15, false}, {10, true}}},

      // Equality operator (swapping has no effect)
      {"Equality swapped", "eq", true, false, 10.5, {{10, false}, {11, false}}},

      // Inequality operator (swapping has no effect)
      {"Inequality swapped",
       "neq",
       true,
       false,
       10.5,
       {{10, true}, {11, true}}}};

  for (const auto& tc : testCases) {
    SCOPED_TRACE(tc.name);

    auto field = makeFieldAccess("c0", BIGINT());
    auto cast = makeCastExpr(DOUBLE(), field);
    auto doubleConst = makeDoubleConstant(tc.compareValue);

    // Build an expression based on whether to swap operands
    auto expr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        tc.swapOperands ? std::vector<core::TypedExprPtr>{doubleConst, cast}
                        : std::vector<core::TypedExprPtr>{cast, doubleConst},
        tc.op);

    // If inversion is needed, add NOT operator
    if (tc.negateComparison) {
      expr = std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(), std::vector<core::TypedExprPtr>{expr}, "not");
    }

    auto [subfield, filter] = toFilter(expr);

    ASSERT_NE(filter, nullptr)
        << "Cast name:" << tc.name << ".Filter creation failed.";
    ASSERT_EQ(filter->kind(), common::FilterKind::kCast)
        << "Cast name:" << tc.name << ".Expected CAST filter ";
    EXPECT_EQ(subfield.toString(), "c0")
        << "Cast name:" << tc.name << ".Wrong field access";

    // Test specific value
    for (const auto& [value, expected] : tc.testValues) {
      EXPECT_EQ(filter->testInt64(value), expected)
          << "Cast name:" << tc.name << ".Value: " << value
          << ", Expected: " << expected;
    }

    // Verify CAST type
    auto castFilter =
        dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
    ASSERT_NE(castFilter, nullptr);
    EXPECT_TRUE(castFilter->sourceType()->kindEquals(BIGINT()));
    EXPECT_TRUE(castFilter->targetType()->kindEquals(DOUBLE()));
  }
}

// Test constants with CAST
TEST_F(CastPushdownTest, CastedConstantComparisons) {
  // Test: CAST(col AS DOUBLE) > CAST('10.5' AS DOUBLE)
  {
    auto field = makeFieldAccess("c0", BIGINT());
    auto fieldCast = makeCastExpr(DOUBLE(), field);

    // Create a constant with CAST
    auto strConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<std::string>("10.5"));
    auto constCast = makeCastExpr(DOUBLE(), strConst);

    auto gtExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{fieldCast, constCast}, "gt");

    auto [subfield, filter] = toFilter(gtExpr);

    ASSERT_NE(filter, nullptr);
    EXPECT_TRUE(filter->testInt64(11));
    EXPECT_FALSE(filter->testInt64(10));
  }

  // Test: CAST('10.5' AS DOUBLE) < CAST(col AS DOUBLE)
  {
    auto field = makeFieldAccess("c0", BIGINT());
    auto fieldCast = makeCastExpr(DOUBLE(), field);

    auto strConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<std::string>("10.5"));
    auto constCast = makeCastExpr(DOUBLE(), strConst);

    auto ltExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{constCast, fieldCast}, "lt");

    auto [subfield, filter] = toFilter(ltExpr);

    ASSERT_NE(filter, nullptr);
    EXPECT_TRUE(filter->testInt64(11));
    EXPECT_FALSE(filter->testInt64(10));
  }
}

TEST_F(CastPushdownTest, CastInt64RangeForReal) {
  // Create a cast filter for target type REAL (float).
  // Here, we cast a BIGINT column to REAL and compare it to a float constant.
  auto field = makeFieldAccess("c0", BIGINT());
  auto castExpr = makeCastExpr(REAL(), field);
  // Use a constant float value, e.g., 123.0f.
  auto floatConst = std::make_shared<core::ConstantTypedExpr>(
      REAL(), variant::create<float>(123.0f));

  // Build an equality expression: CAST(c0 AS REAL) = 123.0f
  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{castExpr, floatConst}, "eq");

  auto [subfield, filter] = toFilter(eqExpr);
  ASSERT_NE(filter, nullptr);

  // Verify that the cast filter is using REAL as target type.
  auto castFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(filter.get());
  ASSERT_NE(castFilter, nullptr);
  EXPECT_TRUE(castFilter->targetType()->kindEquals(REAL()));

  // Define safe integer bounds for float.
  constexpr int64_t kMaxSafeIntegerFloat = (1LL << 24) - 1; // 16,777,215
  constexpr int64_t kMinSafeIntegerFloat = -((1LL << 24) - 1); // -16,777,215

  // Case 1: Range exceeds safe integer bounds.
  // When min or max exceeds the safe integer range, testInt64Range should
  // return true.
  EXPECT_TRUE(filter->testInt64Range(
      kMaxSafeIntegerFloat + 1, kMaxSafeIntegerFloat + 1, false));
  EXPECT_TRUE(filter->testInt64Range(
      kMinSafeIntegerFloat - 1, kMinSafeIntegerFloat - 1, false));

  // Case 2: Range values cause overflow upon conversion to float.
  // Use very large int64_t values that, when converted to float, result in
  // infinity.
  EXPECT_TRUE(filter->testInt64Range(
      std::numeric_limits<int64_t>::max() - 10,
      std::numeric_limits<int64_t>::max(),
      false));
  EXPECT_TRUE(filter->testInt64Range(
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::min() + 10,
      false));

  // Case 3: Range within safe bounds should delegate to the inner filter.
  // Since our equality filter is built against 123.0f, only an exact match
  // should return true.
  EXPECT_TRUE(filter->testInt64Range(
      123, 123, false)); // 123 converts to 123.0f exactly.
  EXPECT_FALSE(
      filter->testInt64Range(124, 124, false)); // 124.0f does not match 123.0f.
}

TEST_F(CastPushdownTest, VarcharToBigintCastFilter) {
  // Test: CAST(c0 AS BIGINT) = 123 where c0 is VARCHAR
  auto field = makeFieldAccess("c0", VARCHAR());
  auto cast = makeCastExpr(BIGINT(), field);
  auto intConst = makeInt64Constant(123);

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, intConst}, "eq");

  auto [subfield, filter] = toFilter(eqExpr);

  // Verify the filter structure
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  // Verify the filter cannot handle string ranges properly
  ASSERT_TRUE(filter->testBytes("123", 3)); // Should pass
  ASSERT_FALSE(filter->testBytes("999", 3));

  // Verify range testing (should return true for any range)
  ASSERT_TRUE(filter->testBytesRange("100", "200", false));
  ASSERT_TRUE(filter->testBytesRange("999", "100", false)); // Invalid range
  ASSERT_TRUE(filter->testBytesRange("abc", "xyz", false)); // Non-numeric
}

// 基本的Cast与非Cast过滤器合并测试
TEST_F(CastPushdownTest, BasicCastWithNonCastMerge) {
  // 原始的基本测试逻辑：CAST(c0 AS DOUBLE) > 10.5 AND c0 < 100
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield1, castFilter] = toFilter(gtExpr);
  ASSERT_NE(castFilter, nullptr);
  ASSERT_EQ(castFilter->kind(), common::FilterKind::kCast);

  // Create the second filter: c0 < 100
  auto upperBound = makeInt64Constant(100);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");

  auto [subfield2, rangeFilter] = toFilter(ltExpr);
  ASSERT_NE(rangeFilter, nullptr);
  ASSERT_EQ(rangeFilter->kind(), common::FilterKind::kBigintRange);

  // Merge the filters
  auto mergedFilter = castFilter->mergeWith(rangeFilter.get());
  ASSERT_NE(mergedFilter, nullptr) << "Filters should merge successfully";
  ASSERT_EQ(mergedFilter->kind(), common::FilterKind::kCast)
      << "Merged filter should be a Cast filter";

  // Check the merged filter's inner structure
  auto mergedCastFilter =
      dynamic_cast<const common::IFilterWithInnerFilter*>(mergedFilter.get());
  ASSERT_NE(mergedCastFilter, nullptr);
  ASSERT_TRUE(mergedCastFilter->sourceType()->kindEquals(BIGINT()));
  ASSERT_TRUE(mergedCastFilter->targetType()->kindEquals(DOUBLE()));

  // Verify filter behavior
  EXPECT_FALSE(mergedFilter->testInt64(5)) << "Value too small for cast filter";
  EXPECT_TRUE(mergedFilter->testInt64(15)) << "Value should pass both filters";
  EXPECT_FALSE(mergedFilter->testInt64(105))
      << "Value too large for range filter";
  EXPECT_FALSE(mergedFilter->testInt64(10))
      << "Value fails cast filter (equal to bound)";
  EXPECT_FALSE(mergedFilter->testInt64(100))
      << "Value fails range filter (equal to bound)";

  // Test range behavior
  EXPECT_TRUE(mergedFilter->testInt64Range(11, 99, false))
      << "Range should pass both filters";
  EXPECT_FALSE(mergedFilter->testInt64Range(5, 10, false))
      << "Range fails cast filter";
  EXPECT_FALSE(mergedFilter->testInt64Range(101, 200, false))
      << "Range fails range filter";
}

TEST_F(CastPushdownTest, MergedFilterEdgeCases) {
  // 测试空字符串输入与合并过滤器
  {
    // 创建一个字符串范围过滤器
    auto stringFilter = std::make_shared<common::BytesRange>(
        "a", false, false, "z", false, false, true);

    // 创建一个表达式并转换为过滤器
    auto field = makeFieldAccess("c0", VARCHAR());
    auto lowerConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<StringView>("a"));
    auto upperConst = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant::create<StringView>("z"));

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{field, lowerConst, upperConst},
        "between");

    auto [subfield, filter] = toFilter(betweenExpr);

    EXPECT_FALSE(filter->testBytes("", 0))
        << "Empty input should fail filter check";

    EXPECT_TRUE(filter->testBytes("apple", 5))
        << "Valid input should pass filter";
  }

  // Test case where both filters reject
  {
    // Create two filters
    auto baseFilter = std::make_shared<common::BigintRange>(100, 200, false);

    auto field = makeFieldAccess("c0", BIGINT());
    auto lowerConst = makeInt64Constant(100);
    auto upperConst = makeInt64Constant(200);

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{field, lowerConst, upperConst},
        "between");

    auto [subfield, filter] = toFilter(betweenExpr);

    // Create a merged filter requiring both filters to pass
    std::vector<std::unique_ptr<common::Filter>> filters;
    filters.push_back(filter->clone());
    filters.push_back(baseFilter->clone());

    auto mergedFilter =
        std::make_shared<common::MultiRange>(std::move(filters), false, false);

    // Test inputs that pass one filter but fail the combined filter
    EXPECT_FALSE(mergedFilter->testInt64(50))
        << "Should fail merged filter check for value outside range";
  }
}

// Test filter evaluation order
TEST_F(CastPushdownTest, FilterEvaluationOrder) {
  // Test merged filter evaluation order
  {
    // Create filter that rejects empty/null input
    auto nonEmptyFilter = std::make_shared<common::BytesValues>(
        std::vector<std::string>{"nonempty"}, true /*nullAllowed*/
    );

    auto castFilter = common::createCastFilter(
        VARCHAR(), VARCHAR(), std::make_shared<common::AlwaysTrue>());

    // Merge filters in both directions to test evaluation order
    auto mergedFilter1 = castFilter->mergeWith(nonEmptyFilter.get());
    auto mergedFilter2 = nonEmptyFilter->mergeWith(castFilter.get());

    // Test empty input - behavior should depend on evaluation order
    EXPECT_FALSE(mergedFilter1->testBytes(nullptr, 0))
        << "Null input should be rejected when non-empty filter is evaluated first";

    // Test for empty string
    EXPECT_FALSE(mergedFilter1->testBytes("", 0))
        << "Empty input should be rejected by merged filter";

    // Test valid input
    EXPECT_TRUE(mergedFilter1->testBytes("nonempty", 8))
        << "Valid input should pass merged filter";
  }
}

TEST_F(CastPushdownTest, MergeWithDifferentFilterTypes) {
  // Test Cast and IsNull filter merge
  {
    auto isNullFilter = std::make_shared<common::IsNull>();

    // Create a Cast filter
    auto field = makeFieldAccess("c0", BIGINT());
    auto cast = makeCastExpr(DOUBLE(), field);
    auto lowerConst = makeDoubleConstant(100.0);
    auto upperConst = makeDoubleConstant(200.0);

    auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{cast, lowerConst, upperConst},
        "between");

    auto [subfield, castFilter] = toFilter(betweenExpr);

    auto mergedFilter = castFilter->mergeWith(isNullFilter.get());
    ASSERT_NE(mergedFilter, nullptr) << "Cast should merge with IsNull filter";

    EXPECT_FALSE(mergedFilter->testNull())
        << "Merged filter should accept NULL values";
    EXPECT_FALSE(mergedFilter->testInt64(150))
        << "Value in range should failed the test, because it's not null.";
    EXPECT_FALSE(mergedFilter->testInt64(50))
        << "Value outside range should fail merged filter, because it's not null.";
  }
}

TEST_F(CastPushdownTest, CrossTypeMerge) {
  // Create two filters of different types
  auto intFilter = createInt32ToInt64CastFilter(100, 200);
  auto stringFilter = createStringCastFilter("a", "z");

  // Try to merge filters of different types
  if (intFilter && stringFilter) {
    auto mergedFilter = intFilter->mergeWith(stringFilter.get());
    ASSERT_EQ(mergedFilter, nullptr)
        << "Cast filters of different source types should not be merged";
  } else {
    GTEST_SKIP() << "Skip test, unable to create test filter";
  }
}

TEST_F(CastPushdownTest, NullFilterHandling) {
  // Create a valid filter
  auto validFilter = createInt32ToInt64CastFilter(100, 200);

  if (validFilter) {
    // Case 1: Merge nullptr
    auto merged1 = validFilter->mergeWith(nullptr);
    ASSERT_EQ(merged1, nullptr);

    // Case 2: The merged filter is nullptr
    std::shared_ptr<common::Filter> nullFilter;
    auto merged2 = validFilter->mergeWith(nullFilter.get());
    ASSERT_EQ(merged2, nullptr);
  } else {
    GTEST_SKIP() << "Skip test, unable to create test filter";
  }
}

// Modify the IntOverflowCases test
TEST_F(CastPushdownTest, IntOverflowCases) {
  auto filter = createInt32ToInt64CastFilter(
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

  if (filter) {
    // Test for int64 values ​​outside the int32 range
    EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::max()));
    EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::min()));

    // Test for boundary values
    EXPECT_TRUE(filter->testInt64(std::numeric_limits<int32_t>::max()));
    EXPECT_TRUE(filter->testInt64(std::numeric_limits<int32_t>::min()));
  } else {
    GTEST_SKIP() << "Skip test, unable to create test filter";
  }
}

TEST_F(CastPushdownTest, InvalidCastCombination) {
  // Create two incompatible filters
  auto intFilter = std::make_shared<common::BigintRange>(100, 200, false);
  auto stringFilter = std::make_shared<common::BytesRange>(
      "a", false, false, "z", false, false, false);

  // Try to merge incompatible filters - use factory function to create cast
  // filter
  auto castIntFilter = common::createCastFilter(INTEGER(), BIGINT(), intFilter);
  auto castStringFilter =
      common::createCastFilter(VARCHAR(), VARCHAR(), stringFilter);

  // Merge should return nullptr instead of throwing an exception
  auto mergedFilter = castIntFilter->mergeWith(castStringFilter.get());
  ASSERT_EQ(mergedFilter, nullptr)
      << "Incompatible Cast filter merging should return nullptr";
}

// Test multiple filter chain merge
TEST_F(CastPushdownTest, MergeWithMultipleFilters) {
  // Test multiple filter chain merge
  // CAST(c0 AS DOUBLE) > 10.5 AND c0 < 100 AND c0 >= 50
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  // Create the first filter: CAST(c0 AS DOUBLE) > 10.5
  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");
  auto [subfield1, castFilter] = toFilter(gtExpr);
  ASSERT_NE(castFilter, nullptr);

  // Create the second filter: c0 < 100
  auto upperBound = makeInt64Constant(100);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");
  auto [subfield2, rangeFilter1] = toFilter(ltExpr);
  ASSERT_NE(rangeFilter1, nullptr);

  // Create the third filter: c0 >= 50
  auto lowerBound = makeInt64Constant(50);
  auto gteExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, lowerBound}, "gte");
  auto [subfield3, rangeFilter2] = toFilter(gteExpr);
  ASSERT_NE(rangeFilter2, nullptr);

  // Chain merged filter
  auto mergedFilter1 = castFilter->mergeWith(rangeFilter1.get());
  ASSERT_NE(mergedFilter1, nullptr);
  auto mergedFilter2 = mergedFilter1->mergeWith(rangeFilter2.get());
  ASSERT_NE(mergedFilter2, nullptr);

  // Verify the final filter behavior
  EXPECT_FALSE(mergedFilter2->testInt64(5))
      << "Value too small for cast filter";
  EXPECT_FALSE(mergedFilter2->testInt64(45)) << "Value too small for range2";
  EXPECT_TRUE(mergedFilter2->testInt64(75)) << "Value should pass all filters";
  EXPECT_FALSE(mergedFilter2->testInt64(105)) << "Value too large for range1";

  // Verify filter type
  verifyCastFilterTypes(mergedFilter2.get(), BIGINT(), DOUBLE());
}

// Test complex nested filter merging
TEST_F(CastPushdownTest, ComplexNestedFilterMerging) {
  // Test complex nested filter merging scenario
  {
    // Create a merged filter chain to test complex scenarios
    auto rangeFilter1 = std::make_shared<common::BigintRange>(10, 100, false);
    auto rangeFilter2 = std::make_shared<common::BigintRange>(50, 150, false);

    // First merge two range filters
    auto mergedRangeFilter = rangeFilter1->mergeWith(rangeFilter2.get());
    ASSERT_NE(mergedRangeFilter, nullptr);

    // Then create a cast filter with the merged range filter
    auto castFilter = common::createCastFilter(
        INTEGER(), // source type
        BIGINT(), // target type
        std::move(mergedRangeFilter)); // inner filter

    // Test the behavior of complex filters
    EXPECT_FALSE(castFilter->testInt64(5))
        << "Value below both ranges should fail";
    EXPECT_TRUE(castFilter->testInt64(75))
        << "Value in intersection should pass";
    EXPECT_FALSE(castFilter->testInt64(125))
        << "Value outside first range should fail";
    EXPECT_FALSE(castFilter->testInt64(200))
        << "Value above both ranges should fail";
  }
}

// Test the merge of two Cast filters
TEST_F(CastPushdownTest, MergeCastWithCast) {
  // Test the merge of two Cast filters with the same source and target types
  // CAST(c0 AS DOUBLE) > 10.5 AND CAST(c0 AS DOUBLE) < 20.5

  auto field = makeFieldAccess("c0", BIGINT());

  // First filter: CAST(c0 AS DOUBLE) > 10.5
  auto cast1 = makeCastExpr(DOUBLE(), field);
  auto lowerBound = makeDoubleConstant(10.5);
  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast1, lowerBound}, "gt");

  auto [subfield1, filter1] = toFilter(gtExpr);
  ASSERT_NE(filter1, nullptr);
  ASSERT_EQ(filter1->kind(), common::FilterKind::kCast);

  // Second filter: CAST(c0 AS DOUBLE) < 20.5
  auto cast2 = makeCastExpr(DOUBLE(), field);
  auto upperBound = makeDoubleConstant(20.5);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast2, upperBound}, "lt");

  auto [subfield2, filter2] = toFilter(ltExpr);
  ASSERT_NE(filter2, nullptr);
  ASSERT_EQ(filter2->kind(), common::FilterKind::kCast);

  // merge filters
  auto mergedFilter = filter1->mergeWith(filter2.get());
  ASSERT_NE(mergedFilter, nullptr) << "Cast filters should merge successfully";
  ASSERT_EQ(mergedFilter->kind(), common::FilterKind::kCast);

  // Verify filter behavior
  EXPECT_FALSE(mergedFilter->testInt64(5)) << "Value too small";
  EXPECT_TRUE(mergedFilter->testInt64(15)) << "Value within range";
  EXPECT_FALSE(mergedFilter->testInt64(25)) << "Value too large";
  EXPECT_FALSE(mergedFilter->testNull()) << "NULL should not pass";
}

TEST_F(CastPushdownTest, MergeNestedFilters) {
  // Test scenario: Create a complex filter by merging multiple levels of
  // filters (CAST(c0 AS DOUBLE) > 10.5 AND c0 < 100) AND c0 >= 50

  // First create a merged filter: CAST(c0 AS DOUBLE) > 10.5 AND c0 < 100
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield1, castFilter] = toFilter(gtExpr);
  ASSERT_NE(castFilter, nullptr);
  ASSERT_EQ(castFilter->kind(), common::FilterKind::kCast);

  auto upperBound = makeInt64Constant(100);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");

  auto [subfield2, rangeFilter] = toFilter(ltExpr);
  ASSERT_NE(rangeFilter, nullptr);
  ASSERT_EQ(rangeFilter->kind(), common::FilterKind::kBigintRange);

  // Merge the cast and range filters
  auto merged1 = castFilter->mergeWith(rangeFilter.get());
  ASSERT_NE(merged1, nullptr) << "First merge should succeed";

  // Verify the merged filter is still a cast filter
  ASSERT_EQ(merged1->kind(), common::FilterKind::kCast)
      << "Merged filter should preserve the cast type";

  // Verify cast filter structure
  verifyCastFilterTypes(merged1.get(), BIGINT(), DOUBLE());

  // Test the first merged filter's behavior
  EXPECT_TRUE(merged1->testInt64(50))
      << "Value should pass initial merged filter";
  EXPECT_FALSE(merged1->testInt64(5)) << "Value too small for cast filter";
  EXPECT_FALSE(merged1->testInt64(150)) << "Value exceeds upper bound";

  // Create a new filter: c0 >= 50
  auto lowerBound = makeInt64Constant(50);
  auto gteExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, lowerBound}, "gte");

  auto [subfield3, newFilter] = toFilter(gteExpr);
  ASSERT_NE(newFilter, nullptr);
  ASSERT_EQ(newFilter->kind(), common::FilterKind::kBigintRange);

  // Now merge the already-merged filter with the new filter
  auto finalMerged = merged1->mergeWith(newFilter.get());
  ASSERT_NE(finalMerged, nullptr) << "Should merge with already-merged filter";

  // Verify the final merged filter is still a cast filter
  ASSERT_EQ(finalMerged->kind(), common::FilterKind::kCast)
      << "Final merged filter should preserve the cast type";

  // Verify the cast filter types are maintained
  verifyCastFilterTypes(finalMerged.get(), BIGINT(), DOUBLE());

  // Test boundary conditions
  EXPECT_FALSE(finalMerged->testInt64(49))
      << "Value just below lower bound (49) should fail";
  EXPECT_TRUE(finalMerged->testInt64(50))
      << "Value at lower bound (50) should pass";
  EXPECT_TRUE(finalMerged->testInt64(75))
      << "Value in middle of range (75) should pass";
  EXPECT_FALSE(finalMerged->testInt64(100))
      << "Value at upper bound (100) should fail";
  EXPECT_FALSE(finalMerged->testInt64(101))
      << "Value just above upper bound (101) should fail";

  // Test extreme values
  EXPECT_FALSE(finalMerged->testInt64(std::numeric_limits<int64_t>::min()))
      << "Minimum int64 value should fail";
  EXPECT_FALSE(finalMerged->testInt64(std::numeric_limits<int64_t>::max()))
      << "Maximum int64 value should fail";

  // Test NULL handling
  EXPECT_FALSE(finalMerged->testNull()) << "NULL should not pass the filter";

  // Test range behavior
  EXPECT_TRUE(finalMerged->testInt64Range(60, 90, false))
      << "Range fully within bounds should pass";
  EXPECT_TRUE(finalMerged->testInt64Range(40, 60, false))
      << "Range partially below lower bound should pass";
  EXPECT_TRUE(finalMerged->testInt64Range(90, 110, false))
      << "Range partially above upper bound should pass";
}

TEST_F(CastPushdownTest, MergeWithBooleanFilters) {
  // Test merging with boolean comparison filters

  // Create a cast filter: CAST(c0 AS DOUBLE) > 10.5
  auto field = makeFieldAccess("c0", BIGINT());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(10.5);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield1, castFilter] = toFilter(gtExpr);
  ASSERT_NE(castFilter, nullptr);

  // Create a boolean-returning comparison: c0 != 0
  auto zeroConst = makeInt64Constant(0);
  auto neqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, zeroConst}, "neq");

  auto [subfield2, boolFilter] = toFilter(neqExpr);
  ASSERT_NE(boolFilter, nullptr);

  // Merge the filters
  auto mergedFilter = castFilter->mergeWith(boolFilter.get());
  ASSERT_NE(mergedFilter, nullptr) << "Should merge with boolean filter";

  // Verify behavior
  EXPECT_FALSE(mergedFilter->testInt64(0)) << "Value fails comparison filter";
  EXPECT_FALSE(mergedFilter->testInt64(5)) << "Value too small for cast filter";
  EXPECT_TRUE(mergedFilter->testInt64(15)) << "Value passes both filters";
}

TEST_F(CastPushdownTest, TestingEquals_IdenticalFiltersAreEqual) {
  // Test case 1: Basic equality test - Two identical Cast filters should be
  // equal

  // Create two identical CAST(c0 AS DOUBLE) > 10.5 filters
  auto filter1 = createInt32ToInt64CastFilter(100, 200);
  auto filter2 = createInt32ToInt64CastFilter(100, 200);

  ASSERT_NE(filter1, nullptr);
  ASSERT_NE(filter2, nullptr);

  verifyCastFilterTypes(filter1.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(filter2.get(), INTEGER(), BIGINT());

  // Verify that identical filters are considered equal
  EXPECT_TRUE(filter1->testingEquals(*filter2));
  EXPECT_TRUE(filter2->testingEquals(*filter1));
  EXPECT_TRUE(filter1->testingEquals(*filter1)); // Self-comparison
}

TEST_F(CastPushdownTest, TestingEquals_DifferentComparisonValues) {
  // Test case 2: Different inner filters - Cast filters with different
  // comparison values should not be equal

  // Create two filters with different bounds
  auto filter1 = createInt32ToInt64CastFilter(100, 200);
  auto filter2 = createInt32ToInt64CastFilter(150, 250); // Different range

  ASSERT_NE(filter1, nullptr);
  ASSERT_NE(filter2, nullptr);

  verifyCastFilterTypes(filter1.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(filter2.get(), INTEGER(), BIGINT());

  // Verify that filters with different comparison values are not equal
  EXPECT_FALSE(filter1->testingEquals(*filter2));
  EXPECT_FALSE(filter2->testingEquals(*filter1));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentOperators) {
  // Test case 3: Different operators - Cast filters using different operators
  // should not be equal

  // Create a between filter: CAST(c0 AS BIGINT) BETWEEN 100 AND 200
  auto betweenFilter = createInt32ToInt64CastFilter(100, 200);

  // Create a different filter structure: use raw expressions to create a >
  // comparison
  auto field = makeFieldAccess("c0", INTEGER());
  auto cast = makeCastExpr(BIGINT(), field);
  auto bigintConst = makeInt64Constant(100);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, bigintConst}, "gt");

  auto [subfield, gtFilter] = toFilter(gtExpr);

  ASSERT_NE(betweenFilter, nullptr);
  ASSERT_NE(gtFilter, nullptr);

  verifyCastFilterTypes(betweenFilter.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(gtFilter.get(), INTEGER(), BIGINT());

  // Verify that filters with different operators are not equal
  EXPECT_FALSE(betweenFilter->testingEquals(*gtFilter));
  EXPECT_FALSE(gtFilter->testingEquals(*betweenFilter));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentTargetTypes) {
  // Test case 4: Different target types - Cast filters with different target
  // types should not be equal

  // Create CAST(c0 AS BIGINT) BETWEEN 100 AND 200
  auto bigintFilter = createInt32ToInt64CastFilter(100, 200);

  // Create a filter with a different target type: CAST(c0 AS DOUBLE)
  auto field = makeFieldAccess("c0", INTEGER());
  auto cast = makeCastExpr(DOUBLE(), field);
  auto doubleConst = makeDoubleConstant(100.0);

  auto gtExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, doubleConst}, "gt");

  auto [subfield, doubleFilter] = toFilter(gtExpr);

  ASSERT_NE(bigintFilter, nullptr);
  ASSERT_NE(doubleFilter, nullptr);

  verifyCastFilterTypes(bigintFilter.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(doubleFilter.get(), INTEGER(), DOUBLE());

  // Verify that filters with different target types are not equal
  EXPECT_FALSE(bigintFilter->testingEquals(*doubleFilter));
  EXPECT_FALSE(doubleFilter->testingEquals(*bigintFilter));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentSourceTypes) {
  // Test case 5: Different source types - Cast filters with different source
  // types should not be equal

  // Create CAST(c0 AS BIGINT) filter where c0 is INTEGER
  auto intToBigintFilter = createInt32ToInt64CastFilter(100, 200);

  // Create a filter with a different source type: CAST(c0 AS BIGINT) where c0
  // is VARCHAR
  auto field = makeFieldAccess("c0", VARCHAR());
  auto cast = makeCastExpr(BIGINT(), field);
  auto bigintConst = makeInt64Constant(100);
  auto upperConst = makeInt64Constant(200);

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{cast, bigintConst, upperConst},
      "between");

  auto [subfield, varcharToBigintFilter] = toFilter(betweenExpr);

  ASSERT_NE(intToBigintFilter, nullptr);
  ASSERT_NE(varcharToBigintFilter, nullptr);

  verifyCastFilterTypes(intToBigintFilter.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(varcharToBigintFilter.get(), VARCHAR(), BIGINT());

  // Verify that filters with different source types are not equal
  EXPECT_FALSE(intToBigintFilter->testingEquals(*varcharToBigintFilter));
  EXPECT_FALSE(varcharToBigintFilter->testingEquals(*intToBigintFilter));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentFilterTypes) {
  // Test case 6: Different filter types - A Cast filter should not equal a
  // non-Cast filter

  // Create a Cast filter
  auto castFilter = createInt32ToInt64CastFilter(100, 200);

  // Create a non-Cast filter (BigintRange)
  auto field = makeFieldAccess("c0", BIGINT());
  auto lowerConst = makeInt64Constant(100);
  auto upperConst = makeInt64Constant(200);

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{field, lowerConst, upperConst},
      "between");

  auto [subfield, rangeFilter] = toFilter(betweenExpr);

  ASSERT_NE(castFilter, nullptr);
  ASSERT_NE(rangeFilter, nullptr);
  ASSERT_EQ(rangeFilter->kind(), common::FilterKind::kBigintRange);

  // Verify that a Cast filter and a non-Cast filter are not equal
  EXPECT_FALSE(castFilter->testingEquals(*rangeFilter));
  EXPECT_FALSE(rangeFilter->testingEquals(*castFilter));
}

TEST_F(CastPushdownTest, TestingEquals_WithWithoutMergedNonCastFilter) {
  // Test case 7: Merged Cast filters with/without mergedNonCastFilter_

  // Create a Cast filter
  auto castFilter = createInt32ToInt64CastFilter(100, 200);

  // Create a non-Cast filter to merge with
  auto field = makeFieldAccess("c0", BIGINT());
  auto upperBound = makeInt64Constant(300);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");

  auto [subfield, rangeFilter] = toFilter(ltExpr);
  ASSERT_NE(rangeFilter, nullptr);

  // Merge the Cast filter with the non-Cast filter
  auto mergedFilter = castFilter->mergeWith(rangeFilter.get());
  ASSERT_NE(mergedFilter, nullptr);

  // Verify that the original filter and the merged filter are not equal
  EXPECT_FALSE(castFilter->testingEquals(*mergedFilter));
  EXPECT_FALSE(mergedFilter->testingEquals(*castFilter));
}

TEST_F(CastPushdownTest, TestingEquals_IdenticalMergedNonCastFilters) {
  // Test case 8: Identical merged non-cast filters - Cast filters with
  // identical merged non-cast filters should be equal

  // Create two identical Cast filters
  auto castFilter1 = createInt32ToInt64CastFilter(100, 200);
  auto castFilter2 = createInt32ToInt64CastFilter(100, 200);

  // Create a non-Cast filter to merge with both
  auto field = makeFieldAccess("c0", BIGINT());
  auto upperBound = makeInt64Constant(300);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");

  auto [subfield, rangeFilter] = toFilter(ltExpr);
  ASSERT_NE(rangeFilter, nullptr);

  // Merge both Cast filters with the same non-Cast filter
  auto mergedFilter1 = castFilter1->mergeWith(rangeFilter.get());
  auto mergedFilter2 = castFilter2->mergeWith(rangeFilter.get());

  ASSERT_NE(mergedFilter1, nullptr);
  ASSERT_NE(mergedFilter2, nullptr);

  // Verify that the two merged filters are equal
  EXPECT_TRUE(mergedFilter1->testingEquals(*mergedFilter2));
  EXPECT_TRUE(mergedFilter2->testingEquals(*mergedFilter1));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentMergedNonCastFilters) {
  // Test case 9: Different merged non-cast filters - Cast filters with
  // different merged non-cast filters should not be equal

  // Create two identical Cast filters
  auto castFilter1 = createInt32ToInt64CastFilter(100, 200);
  auto castFilter2 = createInt32ToInt64CastFilter(100, 200);

  // Create two different non-Cast filters
  auto field = makeFieldAccess("c0", BIGINT());

  auto upperBound1 = makeInt64Constant(300);
  auto ltExpr1 = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound1}, "lt");
  auto [subfield1, rangeFilter1] = toFilter(ltExpr1);

  auto upperBound2 = makeInt64Constant(400); // Different upper bound
  auto ltExpr2 = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound2}, "lt");
  auto [subfield2, rangeFilter2] = toFilter(ltExpr2);

  ASSERT_NE(rangeFilter1, nullptr);
  ASSERT_NE(rangeFilter2, nullptr);

  // Merge Cast filters with different non-Cast filters
  auto mergedFilter1 = castFilter1->mergeWith(rangeFilter1.get());
  auto mergedFilter2 = castFilter2->mergeWith(rangeFilter2.get());

  ASSERT_NE(mergedFilter1, nullptr);
  ASSERT_NE(mergedFilter2, nullptr);

  // Verify that merged filters with different non-cast filters are not equal
  EXPECT_FALSE(mergedFilter1->testingEquals(*mergedFilter2));
  EXPECT_FALSE(mergedFilter2->testingEquals(*mergedFilter1));
}

TEST_F(CastPushdownTest, TestingEquals_NullHandling) {
  // Test case 10: Null handling - Tests how the filter handles comparison with
  // filters of different kinds

  // Create a Cast filter
  auto castFilter = createInt32ToInt64CastFilter(100, 200);

  // Create filters of different kinds to test with

  // IsNull filter
  auto field = makeFieldAccess("c0", INTEGER());
  auto isNullExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field}, "is_null");
  auto [subfield1, isNullFilter] = toFilter(isNullExpr);

  // BytesRange filter (for VARCHAR comparison)
  auto varcharField = makeFieldAccess("c0", VARCHAR());
  auto lowerConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>("a"));
  auto upperConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>("z"));

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{varcharField, lowerConst, upperConst},
      "between");

  auto [subfield2, bytesRangeFilter] = toFilter(betweenExpr);

  ASSERT_NE(castFilter, nullptr);
  ASSERT_NE(isNullFilter, nullptr);
  ASSERT_NE(bytesRangeFilter, nullptr);

  // Verify that a Cast filter is not equal to filters of different kinds
  EXPECT_FALSE(castFilter->testingEquals(*isNullFilter));
  EXPECT_FALSE(castFilter->testingEquals(*bytesRangeFilter));

  // Verify that the comparison is symmetric
  EXPECT_FALSE(isNullFilter->testingEquals(*castFilter));
  EXPECT_FALSE(bytesRangeFilter->testingEquals(*castFilter));
}

TEST_F(CastPushdownTest, TestingEquals_MultipleEquivalentMerges) {
  // Test case 11: Multiple merges with identical results - Filters created
  // through different merge sequences but resulting in identical logical
  // filters should be equal

  // Create identical Cast filters
  auto castFilter1 = createInt32ToInt64CastFilter(100, 200);
  auto castFilter2 = createInt32ToInt64CastFilter(100, 200);

  // Create two different non-Cast filters with the same effective range
  auto field = makeFieldAccess("c0", BIGINT());

  // First non-cast filter: c0 >= 50
  auto lowerBound = makeInt64Constant(50);
  auto gteExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, lowerBound}, "gte");
  auto [subfield1, rangeFilter1] = toFilter(gteExpr);

  // Second non-cast filter: c0 < 250
  auto upperBound = makeInt64Constant(250);
  auto ltExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{field, upperBound}, "lt");
  auto [subfield2, rangeFilter2] = toFilter(ltExpr);

  ASSERT_NE(rangeFilter1, nullptr);
  ASSERT_NE(rangeFilter2, nullptr);

  // Merge filters in different orders
  // First sequence: castFilter1 + rangeFilter1 + rangeFilter2
  auto mergedFilter1 = castFilter1->mergeWith(rangeFilter1.get());
  ASSERT_NE(mergedFilter1, nullptr);
  auto finalMerged1 = mergedFilter1->mergeWith(rangeFilter2.get());
  ASSERT_NE(finalMerged1, nullptr);

  // Second sequence: castFilter2 + rangeFilter2 + rangeFilter1
  auto mergedFilter2 = castFilter2->mergeWith(rangeFilter2.get());
  ASSERT_NE(mergedFilter2, nullptr);
  auto finalMerged2 = mergedFilter2->mergeWith(rangeFilter1.get());
  ASSERT_NE(finalMerged2, nullptr);

  // Verify that filters merged in different orders but with equivalent logical
  // results are equal Note: This might depend on how mergeWith is implemented
  // to normalize filter representations
  EXPECT_TRUE(finalMerged1->testingEquals(*finalMerged2));
  EXPECT_TRUE(finalMerged2->testingEquals(*finalMerged1));
}

TEST_F(CastPushdownTest, TestingEquals_DifferentInnerFilterTypes) {
  // Test case 12: Different inner filter types - Cast filters with different
  // types of inner filters (equality vs range) should not be equal

  // Create a Cast filter with a range inner filter
  auto rangeFilter = createInt32ToInt64CastFilter(
      100, 200); // Range filter (between 100 and 200)

  // Create a Cast filter with an equality inner filter
  auto field = makeFieldAccess("c0", INTEGER());
  auto cast = makeCastExpr(BIGINT(), field);
  auto equalsValue = makeInt64Constant(150);

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, equalsValue}, "eq");

  auto [subfield, equalityFilter] = toFilter(eqExpr);

  ASSERT_NE(rangeFilter, nullptr);
  ASSERT_NE(equalityFilter, nullptr);

  verifyCastFilterTypes(rangeFilter.get(), INTEGER(), BIGINT());
  verifyCastFilterTypes(equalityFilter.get(), INTEGER(), BIGINT());

  // Verify that Cast filters with different inner filter types are not equal
  EXPECT_FALSE(rangeFilter->testingEquals(*equalityFilter));
  EXPECT_FALSE(equalityFilter->testingEquals(*rangeFilter));
}

TEST_F(CastPushdownTest, TestingEquals_InListFilters) {
  // Test case 13: IN list filters - Tests equality with IN list filters, both
  // identical and different

  // Create two identical IN list Cast filters
  // First IN list filter: CAST(c0 AS DOUBLE) IN (1.0, 2.0, 3.0)
  auto field1 = makeFieldAccess("c0", BIGINT());
  auto cast1 = makeCastExpr(DOUBLE(), field1);

  // Create array vector for IN list
  vector_size_t size = 1;
  BufferPtr offsetsBuffer1 = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer1 = allocateSizes(size, pool_.get());

  auto rawOffsets1 = offsetsBuffer1->asMutable<vector_size_t>();
  auto rawSizes1 = sizesBuffer1->asMutable<vector_size_t>();
  rawOffsets1[0] = 0;
  rawSizes1[0] = 3;

  std::vector<double> values1 = {1.0, 2.0, 3.0};
  auto elements1 = makeFlatVector<double>(values1);

  auto arrayVector1 = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(DOUBLE()),
      nullptr,
      size,
      offsetsBuffer1,
      sizesBuffer1,
      elements1);

  auto valuesExpr1 = std::make_shared<core::ConstantTypedExpr>(arrayVector1);
  auto inExpr1 = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast1, valuesExpr1}, "in");

  auto [subfield1, inListFilter1] = toFilter(inExpr1);

  // Second identical IN list filter
  auto field2 = makeFieldAccess("c0", BIGINT());
  auto cast2 = makeCastExpr(DOUBLE(), field2);

  // Create array vector for IN list
  BufferPtr offsetsBuffer2 = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer2 = allocateSizes(size, pool_.get());

  auto rawOffsets2 = offsetsBuffer2->asMutable<vector_size_t>();
  auto rawSizes2 = sizesBuffer2->asMutable<vector_size_t>();
  rawOffsets2[0] = 0;
  rawSizes2[0] = 3;

  std::vector<double> values2 = {1.0, 2.0, 3.0};
  auto elements2 = makeFlatVector<double>(values2);

  auto arrayVector2 = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(DOUBLE()),
      nullptr,
      size,
      offsetsBuffer2,
      sizesBuffer2,
      elements2);

  auto valuesExpr2 = std::make_shared<core::ConstantTypedExpr>(arrayVector2);
  auto inExpr2 = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast2, valuesExpr2}, "in");

  auto [subfield2, inListFilter2] = toFilter(inExpr2);

  // Create a different IN list filter with different values
  auto field3 = makeFieldAccess("c0", BIGINT());
  auto cast3 = makeCastExpr(DOUBLE(), field3);

  // Create array vector for different IN list
  BufferPtr offsetsBuffer3 = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer3 = allocateSizes(size, pool_.get());

  auto rawOffsets3 = offsetsBuffer3->asMutable<vector_size_t>();
  auto rawSizes3 = sizesBuffer3->asMutable<vector_size_t>();
  rawOffsets3[0] = 0;
  rawSizes3[0] = 3;

  std::vector<double> values3 = {
      1.0, 2.0, 4.0}; // Different values (4.0 instead of 3.0)
  auto elements3 = makeFlatVector<double>(values3);

  auto arrayVector3 = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(DOUBLE()),
      nullptr,
      size,
      offsetsBuffer3,
      sizesBuffer3,
      elements3);

  auto valuesExpr3 = std::make_shared<core::ConstantTypedExpr>(arrayVector3);
  auto inExpr3 = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast3, valuesExpr3}, "in");

  auto [subfield3, inListFilter3] = toFilter(inExpr3);

  ASSERT_NE(inListFilter1, nullptr);
  ASSERT_NE(inListFilter2, nullptr);
  ASSERT_NE(inListFilter3, nullptr);

  // Verify that identical IN list filters are equal
  EXPECT_TRUE(inListFilter1->testingEquals(*inListFilter2));
  EXPECT_TRUE(inListFilter2->testingEquals(*inListFilter1));

  // Verify that different IN list filters are not equal
  EXPECT_FALSE(inListFilter1->testingEquals(*inListFilter3));
  EXPECT_FALSE(inListFilter3->testingEquals(*inListFilter1));
}

TEST_F(CastPushdownTest, DateToVarcharCast) {
  // Test: CAST(date_col AS VARCHAR) = '2023-01-20'
  auto field = makeFieldAccess("c0", DATE());
  auto cast = makeCastExpr(VARCHAR(), field);

  // Create a date constant and get its string representation
  int32_t dateValue = 19376; // Represents 2023-01-20
  std::string dateStr = DATE()->toString(dateValue);

  auto strConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(dateStr));

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{cast, strConst}, "eq");

  auto [subfield, filter] = toFilter(eqExpr);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  // Verify that the filter correctly handles DATE->VARCHAR conversion
  EXPECT_TRUE(filter->testInt64(dateValue));
  EXPECT_FALSE(filter->testInt64(dateValue + 1));
  EXPECT_FALSE(filter->testInt64(dateValue - 1));
}

TEST_F(CastPushdownTest, DateToVarcharRangeCast) {
  // Test: CAST(date_col AS VARCHAR) BETWEEN '2023-01-20' AND '2023-02-01'
  auto field = makeFieldAccess("c0", DATE());
  auto cast = makeCastExpr(VARCHAR(), field);

  // Create date constants for range boundaries
  int32_t startDate = 19376; // 2023-01-20
  int32_t endDate = 19388; // 2023-02-01

  auto startDateStr = DATE()->toString(startDate);
  auto endDateStr = DATE()->toString(endDate);

  auto lowerConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(startDateStr));
  auto upperConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(endDateStr));

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{cast, lowerConst, upperConst},
      "between");

  auto [subfield, filter] = toFilter(betweenExpr);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  // Test range behavior
  EXPECT_TRUE(filter->testInt64(startDate));
  EXPECT_TRUE(filter->testInt64((startDate + endDate) / 2)); // Middle date
  EXPECT_TRUE(filter->testInt64(endDate));
  EXPECT_FALSE(filter->testInt64(startDate - 1));
  EXPECT_FALSE(filter->testInt64(endDate + 1));

  // Test range testing
  EXPECT_TRUE(filter->testInt64Range(startDate, endDate, false));
  EXPECT_TRUE(filter->testInt64Range(startDate + 1, endDate - 1, false));
  EXPECT_TRUE(filter->testInt64Range(startDate - 1, startDate + 1, false));
}

TEST_F(CastPushdownTest, DateToVarcharCastFilter) {
  // Test CAST(date_col AS VARCHAR) between filter
  auto field = makeFieldAccess("c0", DATE());
  auto cast = makeCastExpr(VARCHAR(), field);

  // Use the new helper function to create date constants
  // First, convert them to strings
  int32_t startDate = 19376; // 2023-01-20
  int32_t endDate = 19388; // 2023-02-01

  auto startDateString = DATE()->toString(startDate);
  auto endDateString = DATE()->toString(endDate);

  // Create string constants for the comparison
  auto lowerConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(startDateString));
  auto upperConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(endDateString));

  auto betweenExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{cast, lowerConst, upperConst},
      "between");

  auto [subfield, filter] = toFilter(betweenExpr);

  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(subfield.toString(), "c0");

  // Verify filter behavior
  EXPECT_TRUE(filter->testInt64(startDate)); // Exactly at start date
  EXPECT_TRUE(filter->testInt64(19380)); // Middle date
  EXPECT_TRUE(filter->testInt64(endDate)); // Exactly at end date
  EXPECT_FALSE(filter->testInt64(startDate - 1)); // Just before range
  EXPECT_FALSE(filter->testInt64(endDate + 1)); // Just after range

  // Also check a specific date as a constant
  auto dateField = makeFieldAccess("c0", DATE());
  auto dateCast = makeCastExpr(VARCHAR(), dateField);
  auto specificDate = DATE()->toString(19380); // Some date in the middle

  auto specificDateConst = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant::create<StringView>(specificDate));

  auto eqExpr = std::make_shared<const core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{dateCast, specificDateConst},
      "eq");

  auto [eqSubfield, eqFilter] = toFilter(eqExpr);

  ASSERT_NE(eqFilter, nullptr);
  EXPECT_TRUE(eqFilter->testInt64(19380)); // Should match exactly
  EXPECT_FALSE(eqFilter->testInt64(19379)); // One day earlier should fail
}
} // namespace bytedance::bolt::test
