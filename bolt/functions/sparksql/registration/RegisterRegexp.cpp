#include "bolt/functions/lib/Re2Functions.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/ICURegexFunctions.h"
#include "bolt/functions/sparksql/RegexFunctions.h"
namespace bytedance::bolt::functions::sparksql {

void registerRegexpFunctions(const std::string& prefix) {
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "rlike", re2SearchSignatures(), makeRLike);

  // using ICU
  exec::registerStatefulVectorFunction(
      prefix + "icu_regexp_replace",
      icuRegexReplaceSignatures(),
      makeICURegexReplace);
  exec::registerStatefulVectorFunction(
      prefix + "icu_regexp_extract",
      icuRegexExtractSignatures(),
      makeICURegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "icu_regexp_extract_all",
      icuRegexExtractAllSignatures(),
      makeICURegexExtractAll);
  exec::registerStatefulVectorFunction(
      prefix + "icu_regexp", icuRegexLikeSignatures(), makeICURegexLike);
  exec::registerStatefulVectorFunction(
      prefix + "icu_rlike", icuRegexLikeSignatures(), makeICURegexLike);
  exec::registerStatefulVectorFunction(
      prefix + "icu_regexp_like", icuRegexLikeSignatures(), makeICURegexLike);

  BOLT_REGISTER_VECTOR_FUNCTION(udf_regexp_split, prefix + "split");
}
} // namespace bytedance::bolt::functions::sparksql