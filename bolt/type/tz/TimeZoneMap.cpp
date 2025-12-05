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

#include "bolt/type/tz/TimeZoneMap.h"

#include <boost/algorithm/string.hpp>
#include <date/date.h>
#include <fmt/core.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::util {

// Defined on TimeZoneDatabase.cpp
extern const std::unordered_map<int64_t, std::string>& getTimeZoneDB();
extern const folly::F14FastMap<std::string, int64_t>& getTimeZoneIdDB();

namespace {

folly::F14FastMap<std::string, int64_t> makeReverseMap(
    const std::unordered_map<int64_t, std::string>& map) {
  folly::F14FastMap<std::string, int64_t> reversed;
  reversed.reserve(map.size() + 1);

  for (const auto& entry : map) {
    reversed.emplace(
        boost::algorithm::to_lower_copy(entry.second), entry.first);
  }
  reversed.emplace("utc", 0);
  return reversed;
}

inline bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

inline bool startsWith(std::string_view str, const char* prefix) {
  return str.rfind(prefix, 0) == 0;
}

// The timezone parsing logic follows what is defined here:
//   https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
inline bool isUtcEquivalentName(std::string_view zone) {
  static folly::F14FastSet<std::string> utcSet = {
      "utc", "uct", "gmt", "gmt0", "greenwich", "universal", "zulu", "ut", "z"};
  return utcSet.find(zone) != utcSet.end();
}

std::string normalizeTimeZone(const std::string& originalZoneId) {
  std::string_view zoneId = originalZoneId;
  const bool startsWithEtc = startsWith(zoneId, "etc/");

  if (startsWithEtc) {
    zoneId = zoneId.substr(4);
  }

  // ETC/GMT, ETC/GREENWICH, and others are all valid and link to GMT.
  if (isUtcEquivalentName(zoneId)) {
    return "utc";
  }

  // Check for Etc/GMT(+/-)H[H] pattern.
  if (startsWithEtc) {
    if (zoneId.size() > 4 && startsWith(zoneId, "gmt")) {
      zoneId = zoneId.substr(3);
      char signChar = zoneId[0];

      if (signChar == '+' || signChar == '-') {
        // ETC flips the sign.
        signChar = (signChar == '-') ? '+' : '-';

        // Extract the tens and ones characters for the hour.
        char hourTens;
        char hourOnes;

        if (zoneId.size() == 2) {
          hourTens = '0';
          hourOnes = zoneId[1];
        } else {
          hourTens = zoneId[1];
          hourOnes = zoneId[2];
        }

        // Prevent it from returning -00:00, which is just utc.
        if (hourTens == '0' && hourOnes == '0') {
          return "utc";
        }

        if (isDigit(hourTens) && isDigit(hourOnes)) {
          return std::string() + signChar + hourTens + hourOnes + ":00";
        }
      }
    }
  }
  return originalZoneId;
}

} // namespace

template <typename TDuration>
void validateRangeImpl(time_point<TDuration> timePoint) {
  static constexpr auto kMinYear = ::date::year::min();
  static constexpr auto kMaxYear = ::date::year::max();

  auto year =
      ::date::year_month_day(::date::floor<::date::days>(timePoint)).year();

  if (year < kMinYear || year > kMaxYear) {
    // This is a special case where we intentionally throw
    // BoltRuntimeError to avoid it being suppressed by TRY().
    BOLT_FAIL(
        "Timepoint is outside of supported year range: [{}, {}], got {}",
        static_cast<int>(kMinYear),
        static_cast<int>(kMaxYear),
        static_cast<int>(year));
  }
}

void validateRange(time_point<std::chrono::seconds> timePoint) {
  validateRangeImpl(timePoint);
}

void validateRange(time_point<std::chrono::milliseconds> timePoint) {
  validateRangeImpl(timePoint);
}

std::string getTimeZoneName(int64_t timeZoneID) {
  const auto& tzDB = getTimeZoneDB();
  auto it = tzDB.find(timeZoneID);
  BOLT_CHECK(it != tzDB.end(), "Unable to resolve timeZoneID '{}'", timeZoneID);
  return it->second;
}

int64_t getTimeZoneID(std::string_view timeZone, bool failOnError) {
  static folly::F14FastMap<std::string, int64_t> nameToIdMap =
      getTimeZoneIdDB();
  std::string timeZoneLowered;
  boost::algorithm::to_lower_copy(
      std::back_inserter(timeZoneLowered), timeZone);

  auto it = nameToIdMap.find(timeZoneLowered);
  if (it != nameToIdMap.end()) {
    return it->second;
  }

  // If an exact match wasn't found, try to normalize the timezone name.
  it = nameToIdMap.find(normalizeTimeZone(timeZoneLowered));
  if (it != nameToIdMap.end()) {
    return it->second;
  }
  if (failOnError) {
    BOLT_FAIL("Unknown time zone: '{}'", timeZone);
  }
  return -1;
}

bool isDstTimezone(int16_t tzID) {
  // for daylight saving time
  static const folly::F14FastSet<int16_t> dstTzID{
      1698, // Africa/Dakar
      1825, // America/Los_Angeles
      2056 // Europe/Brussels
  };
  if (dstTzID.find(tzID) != dstTzID.end()) {
    return true;
  }
  return false;
}

} // namespace bytedance::bolt::util
