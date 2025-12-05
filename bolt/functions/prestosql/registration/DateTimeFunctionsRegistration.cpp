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

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/DateTimeFunctions.h"
#include "bolt/functions/sparksql/DateTimeFunctions.h"
namespace bytedance::bolt::functions {
namespace {
void registerSimpleFunctions(const std::string& prefix) {
  // Date time functions.
  registerFunction<ToUnixtimeFunction, double, Timestamp>(
      {prefix + "to_unixtime"});
  registerFunction<ToUnixtimeFunction, double, TimestampWithTimezone>(
      {prefix + "to_unixtime"});
  registerFunction<FromUnixtimeFunction, Timestamp, double>(
      {prefix + "from_unixtime"});
  registerFunction<FromUnixtimeFunction, Varchar, int64_t>(
      {prefix + "from_unixtime"});
  registerFunction<FromUnixtimeFunction, Varchar, int64_t, Varchar>(
      {prefix + "from_unixtime"});

  registerFunction<DateFunction, Date, Varchar>({
      prefix + "date",
      prefix + "to_date",
  });
  registerFunction<DateFunction, Date, Timestamp>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, TimestampWithTimezone>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, int64_t>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, Date>(
      {prefix + "date", prefix + "to_date"});

  registerFunction<TimeZoneHourFunction, int64_t, TimestampWithTimezone>(
      {prefix + "timezone_hour"});
  registerFunction<TimeZoneMinuteFunction, int64_t, TimestampWithTimezone>(
      {prefix + "timezone_minute"});
  registerFunction<ToTimestampFunction, Timestamp, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampFunction, Timestamp, int64_t>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampFunction, Timestamp, int32_t>(
      {prefix + "to_timestamp"});

#ifndef SPARK_COMPATIBLE
  registerFunction<YearFunction, int64_t, Date>({prefix + "year"});
  registerFunction<YearFunction, int64_t, Timestamp>({prefix + "year"});
  registerFunction<QuarterFunction, int64_t, Timestamp>({prefix + "quarter"});
  registerFunction<QuarterFunction, int64_t, Date>({prefix + "quarter"});
  registerFunction<MonthFunction, int64_t, Timestamp>({prefix + "month"});
  registerFunction<MonthFunction, int64_t, Date>({prefix + "month"});
  registerFunction<WeekFunction, int64_t, Timestamp>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<WeekFunction, int64_t, Date>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayOfWeekFunction,
      int64_t,
      Date>({prefix + "dayofweek"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayOfWeekFunction,
      int64_t,
      Timestamp>({prefix + "dayofweek"});
  registerFunction<HourFunction, int64_t, Timestamp>({prefix + "hour"});
  registerFunction<MinuteFunction, int64_t, Timestamp>({prefix + "minute"});
  registerFunction<SecondFunction, int64_t, Timestamp>({prefix + "second"});
  registerFunction<MillisecondFunction, int64_t, Timestamp>(
      {prefix + "millisecond"});
#endif
  registerFunction<YearFunction, int64_t, TimestampWithTimezone>(
      {prefix + "year"});
  registerFunction<WeekFunction, int32_t, Date>({prefix + "weekofyear"});
  registerFunction<WeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<QuarterFunction, int64_t, TimestampWithTimezone>(
      {prefix + "quarter"});
  registerFunction<MonthFunction, int64_t, TimestampWithTimezone>(
      {prefix + "month"});
  registerFunction<DayFunction, int64_t, Timestamp>(
      {prefix + "day", prefix + "day_of_month"});
  registerFunction<DayFunction, int64_t, Date>(
      {prefix + "day", prefix + "day_of_month"});
  registerFunction<DateMinusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "minus"});
  registerFunction<DatePlusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "plus"});
  registerFunction<
      TimestampMinusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "minus"});
  registerFunction<
      TimestampPlusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "plus"});
  registerFunction<
      IntervalDayTimePlusTimestamp,
      Timestamp,
      IntervalDayTime,
      Timestamp>({prefix + "plus"});
  registerFunction<
      TimestampMinusFunction,
      IntervalDayTime,
      Timestamp,
      Timestamp>({prefix + "minus"});
  registerFunction<DayFunction, int64_t, TimestampWithTimezone>(
      {prefix + "day", prefix + "day_of_month"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayFunction,
      int32_t,
      Date>({prefix + "dayofmonth"});
  registerFunction<DayOfWeekFunction, int64_t, Timestamp>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfWeekFunction, int64_t, Date>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfWeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfYearFunction, int64_t, Timestamp>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<DayOfYearFunction, int64_t, Date>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<DayOfYearFunction, int64_t, TimestampWithTimezone>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayOfYearFunction,
      int32_t,
      Date>({prefix + "dayofyear"});
  registerFunction<YearOfWeekFunction, int64_t, Timestamp>(
      {prefix + "yow", prefix + "year_of_week"});
  registerFunction<YearOfWeekFunction, int64_t, Date>(
      {prefix + "yow", prefix + "year_of_week"});
  registerFunction<YearOfWeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "yow", prefix + "year_of_week"});
  registerFunction<HourFunction, int64_t, Date>({prefix + "hour"});
  registerFunction<HourFunction, int64_t, TimestampWithTimezone>(
      {prefix + "hour"});
  registerFunction<LastDayOfMonthFunction, Date, Timestamp>(
      {prefix + "last_day_of_month"});
  registerFunction<LastDayOfMonthFunction, Date, Date>(
      {prefix + "last_day_of_month"});
  registerFunction<LastDayOfMonthFunction, Date, TimestampWithTimezone>(
      {prefix + "last_day_of_month"});
  registerFunction<MinuteFunction, int64_t, Date>({prefix + "minute"});
  registerFunction<MinuteFunction, int64_t, TimestampWithTimezone>(
      {prefix + "minute"});
  registerFunction<SecondFunction, int64_t, Date>({prefix + "second"});
  registerFunction<SecondFunction, int64_t, TimestampWithTimezone>(
      {prefix + "second"});
  registerFunction<MillisecondFunction, int64_t, Date>(
      {prefix + "millisecond"});
  registerFunction<MillisecondFunction, int64_t, TimestampWithTimezone>(
      {prefix + "millisecond"});
#ifndef SPARK_COMPATIBLE
  registerFunction<DateTruncFunction, Timestamp, Varchar, Timestamp>(
      {prefix + "date_trunc"});
  registerFunction<DateTruncFunction, Date, Varchar, Date>(
      {prefix + "date_trunc"});
  registerFunction<
      DateTruncFunction,
      TimestampWithTimezone,
      Varchar,
      TimestampWithTimezone>({prefix + "date_trunc"});
  registerFunction<DateTruncFunction, Date, Date, Varchar>({prefix + "trunc"});
  registerFunction<DateTruncFunction, Date, Timestamp, Varchar>(
      {prefix + "trunc"});
  registerFunction<DateTruncFunction, Date, TimestampWithTimezone, Varchar>(
      {prefix + "trunc"});
  registerFunction<DateTruncFunction, Date, Varchar, Varchar>(
      {prefix + "trunc"});
#endif
  registerFunction<DateAddFunction, Date, Varchar, int64_t, Date>(
      {prefix + "date_add"});
  registerFunction<DateAddFunction, Timestamp, Varchar, int64_t, Timestamp>(
      {prefix + "date_add", prefix + "timestampadd"});
  registerFunction<DateAddFunction, Timestamp, Varchar, int32_t, Timestamp>(
      {prefix + "date_add", prefix + "timestampadd"});
  registerFunction<
      DateAddFunction,
      TimestampWithTimezone,
      Varchar,
      int64_t,
      TimestampWithTimezone>({prefix + "date_add", prefix + "timestampadd"});

  registerFunction<DateAddFunction, Date, Date, int64_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int32_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int16_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int8_t>({prefix + "date_add"});

#ifndef SPARK_COMPATIBLE
  registerFunction<
      DateDiffFunction,
      int64_t,
      Varchar,
      TimestampWithTimezone,
      TimestampWithTimezone>({prefix + "date_diff"});
  registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
      {prefix + "date_diff"});
  registerFunction<DateDiffFunction, int64_t, Date, Date>(
      {prefix + "date_diff"});
  registerFunction<DateFormatFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});
  registerFunction<DateFormatFunction, Varchar, TimestampWithTimezone, Varchar>(
      {prefix + "date_format"});
  registerFunction<sparksql::LastDayFunction, Varchar, Date>(
      {prefix + "last_day"});
#endif
  registerFunction<HiveDateDiffFunction, int32_t, Date, Date>(
      {prefix + "datediff"});
  registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
      {prefix + "timestampdiff"});

  registerFunction<FormatDateTimeFunction, Varchar, Timestamp, Varchar>(
      {prefix + "format_datetime"});
  registerFunction<
      FormatDateTimeFunction,
      Varchar,
      TimestampWithTimezone,
      Varchar>({prefix + "format_datetime"});
  registerFunction<
      ParseDateTimeFunction,
      TimestampWithTimezone,
      Varchar,
      Varchar>({prefix + "parse_datetime"});
  registerFunction<DateParseFunction, Timestamp, Varchar, Varchar>(
      {prefix + "date_parse"});
  registerFunction<CurrentDateFunction, Date>({prefix + "current_date"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, Varchar>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, int64_t>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, int32_t>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::PDate2DateFunction, Varchar, Varchar>(
      {prefix + "pdate2date"});
  registerFunction<sparksql::PDate2DateFunction, Varchar, Date>(
      {prefix + "pdate2date"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampFunction,
      int64_t>({prefix + "unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Date>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Timestamp>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::
          UnixTimestampParseWithFormatFunction,
      int64_t,
      Varchar,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampFromTimestampFunction,
      int64_t,
      Timestamp,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<MonthsBetweenFunction, double, Timestamp, Timestamp>(
      {prefix + "months_between"});
  registerFunction<
      bytedance::bolt::functions::sparksql::NextDayFunction,
      Date,
      Date,
      Varchar>({prefix + "next_day"});
}
} // namespace

void registerDateTimeFunctions(const std::string& prefix) {
  registerTimestampWithTimeZoneType();

  registerSimpleFunctions(prefix);
  BOLT_REGISTER_VECTOR_FUNCTION(udf_from_unixtime, prefix + "from_unixtime");
}
} // namespace bytedance::bolt::functions
