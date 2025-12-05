#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/JsonFunctions.h"
#include "bolt/functions/prestosql/SIMDJsonFunctions.h"
#include "bolt/functions/sparksql/JsonObjectKeys.h"
#include "bolt/functions/sparksql/JsonTuple.h"
#include "bolt/functions/sparksql/SIMDJsonFunctions.h"
#include "bolt/functions/sparksql/specialforms/FromJson.h"
#include "bolt/functions/sparksql/specialforms/JsonSplit.h"
namespace bytedance::bolt::functions {
static void registerSparkJsonFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_to_json, prefix + "to_json");
}
namespace sparksql {
void registerJsonFunctions(const std::string& prefix) {
  registerSparkJsonFunctions(prefix);
  registerFunctionCallToSpecialForm(
      "json_split", std::make_unique<JsonSplitToSpecialForm>());
  //   registerFunctionCallToSpecialForm(
  //       "from_json", std::make_unique<FromJsonToSpecialForm>());

  BOLT_REGISTER_VECTOR_FUNCTION(udf_json_to_map, prefix + "json_to_map");
  exec::registerStatefulVectorFunction(
      prefix + "json_tuple_with_codegen", jsonTupleSignatures(), makeJsonTuple);
  registerJsonType(); // to register Json type
  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Json>(
      {prefix + "json_array_length"});
  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Varchar>(
      {prefix + "json_array_length"});

  //   registerFunction<SIMDGetJsonObjectFunction, Varchar, Varchar, Varchar>(
  //       {prefix + "get_json_object"});

  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  registerFunction<JsonObjectKeysFunction, Array<Varchar>, Varchar>(
      {prefix + "json_object_keys"});
}
} // namespace sparksql
} // namespace bytedance::bolt::functions