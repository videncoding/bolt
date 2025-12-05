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

#include "bolt/functions/lib/CheckedArithmetic.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Arithmetic.h"
#include "bolt/functions/sparksql/Arithmetic.h"
#include "bolt/functions/sparksql/Factorial.h"
#include "bolt/functions/sparksql/Rand.h"
namespace bytedance::bolt::functions {

void registerSparkMathFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_abs, prefix + "abs");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_negate, prefix + "negative");
}
namespace sparksql {
void registerMathFunctions(const std::string& prefix) {
  registerSparkMathFunctions(prefix);
  registerFunction<RandFunction, double>({prefix + "rand", prefix + "random"});
  registerFunction<RandFunction, double, Constant<int32_t>>(
      {prefix + "rand", prefix + "random"});
  registerFunction<RandFunction, double, Constant<int64_t>>(
      {prefix + "rand", prefix + "random"});

  // Operators.
  registerBinaryNumeric<PlusFunction>({prefix + "add"});
  registerBinaryNumeric<MinusFunction>({prefix + "subtract"});
  registerBinaryNumeric<MultiplyFunction>({prefix + "multiply"});
  registerFunction<DivideFunction, double, double, double>({prefix + "divide"});
  registerBinaryNumeric<RemainderFunction>({prefix + "remainder"});
  registerUnaryNumeric<UnaryMinusFunction>({prefix + "unaryminus"});
  // Math functions.
  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerUnaryNumeric<NegateFunction>({prefix + "negative"});
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<AcoshFunction, double, double>({prefix + "acosh"});
  registerFunction<AsinhFunction, double, double>({prefix + "asinh"});
  registerFunction<AtanhFunction, double, double>({prefix + "atanh"});
  registerFunction<SecFunction, double, double>({prefix + "sec"});
  registerFunction<CscFunction, double, double>({prefix + "csc"});
  registerFunction<SinhFunction, double, double>({prefix + "sinh"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<CotFunction, double, double>({prefix + "cot"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});
  registerFunction<Log1pFunction, double, double>({prefix + "log1p"});
  registerFunction<ToBinaryStringFunction, Varchar, int64_t>({prefix + "bin"});
  registerFunction<ToHexBigintFunction, Varchar, int64_t>({prefix + "hex"});
  registerFunction<ToHexVarcharFunction, Varchar, Varchar>({prefix + "hex"});
  registerFunction<ToHexVarbinaryFunction, Varchar, Varbinary>(
      {prefix + "hex"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerFunction<Expm1Function, double, double>({prefix + "expm1"});
  registerBinaryIntegral<PModIntFunction>({prefix + "pmod"});
  registerBinaryFloatingPoint<PModFloatFunction>({prefix + "pmod"});
  registerFunction<PowerFunction, double, double, double>({prefix + "power"});
  registerFunction<RIntFunction, double, double>({prefix + "rint"});
  registerUnaryNumeric<RoundFunction>({prefix + "round"});
  registerFunction<RoundFunction, int8_t, int8_t, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, int16_t, int16_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int32_t, int32_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int64_t, int64_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, double, double, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, float, float, int32_t>({prefix + "round"});
  registerFunction<UnHexFunction, Varbinary, Varchar>({prefix + "unhex"});
  // In Spark only long, double, and decimal have ceil/floor
  registerFunction<sparksql::CeilFunction, int64_t, int64_t>({prefix + "ceil"});
  registerFunction<sparksql::CeilFunction, int64_t, double>({prefix + "ceil"});
  registerFunction<sparksql::FloorFunction, int64_t, int64_t>(
      {prefix + "floor"});
  registerFunction<sparksql::FloorFunction, int64_t, double>(
      {prefix + "floor"});
  registerFunction<HypotFunction, double, double, double>({prefix + "hypot"});
  registerFunction<LnFunction, double, double>({prefix + "log"});
  registerFunction<LogFunction, double, double, double>({prefix + "log"});
  registerFunction<LnFunction, double, double>({prefix + "ln"});
  registerFunction<Log2Function, double, double>({prefix + "log2"});
  registerFunction<Log10Function, double, double>({prefix + "log10"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({prefix + "width_bucket"});

  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "add");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_sub, prefix + "subtract");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_mul, prefix + "multiply");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_div, prefix + "divide");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_add_deny_precision_loss, prefix + "add_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_sub_deny_precision_loss,
      prefix + "subtract_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_mul_deny_precision_loss,
      prefix + "multiply_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_div_deny_precision_loss,
      prefix + "divide_deny_precision_loss");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_ceil, prefix + "ceil");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_floor, prefix + "floor");

  registerFunction<sparksql::IsNanFunction, bool, float>({prefix + "isnan"});
  registerFunction<sparksql::IsNanFunction, bool, double>({prefix + "isnan"});
  registerFunction<sparksql::Atan2Function, double, double, double>(
      {prefix + "atan2"});
  registerFunction<sparksql::FactorialFunction, int64_t, int32_t>(
      {prefix + "factorial"});
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
