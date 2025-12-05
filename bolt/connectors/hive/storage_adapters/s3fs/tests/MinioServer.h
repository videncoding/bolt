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

#include "bolt/common/config/Config.h"
#include "bolt/exec/tests/utils/PortUtil.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include "boost/process.hpp"
using namespace bytedance::bolt;

namespace {
constexpr char const* kMinioExecutableName{"minio"};
constexpr char const* kMinioAccessKey{"minio"};
constexpr char const* kMinioSecretKey{"miniopass"};
} // namespace

// A minio server, managed as a child process.
// Adapted from the Apache Arrow library.
class MinioServer {
 public:
  MinioServer(const std::string_view& connectionString)
      : tempPath_(::exec::test::TempDirectoryPath::create()),
        connectionString_(connectionString) {}

  void start();

  void stop();

  void addBucket(const char* bucket) {
    const std::string path = tempPath_->path + "/" + bucket;
    mkdir(path.c_str(), S_IRWXU | S_IRWXG);
  }

  std::string path() const {
    return tempPath_->path;
  }

  std::shared_ptr<const config::ConfigBase> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {})
      const {
    std::unordered_map<std::string, std::string> config({
        {"hive.s3.aws-access-key", accessKey_},
        {"hive.s3.aws-secret-key", secretKey_},
        {"hive.s3.endpoint", connectionString_},
        {"hive.s3.ssl.enabled", "false"},
        {"hive.s3.path-style-access", "true"},
    });

    // Update the default config map with the supplied configOverride map
    for (const auto& [configName, configValue] : configOverride) {
      config[configName] = configValue;
    }

    return std::make_shared<const config::ConfigBase>(std::move(config));
  }

 private:
  const std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
  const std::string connectionString_;
  const std::string accessKey_ = kMinioAccessKey;
  const std::string secretKey_ = kMinioSecretKey;
  std::shared_ptr<::boost::process::child> serverProcess_;
};

void MinioServer::start() {
  boost::process::environment env = boost::this_process::environment();
  env["MINIO_ACCESS_KEY"] = accessKey_;
  env["MINIO_SECRET_KEY"] = secretKey_;

  auto exePath = boost::process::search_path(kMinioExecutableName);
  if (exePath.empty()) {
    BOLT_FAIL("Failed to find minio executable {}'", kMinioExecutableName);
  }

  try {
    serverProcess_ = std::make_shared<boost::process::child>(
        env,
        exePath,
        "server",
        "--quiet",
        "--compat",
        "--address",
        connectionString_,
        tempPath_->path.c_str());
  } catch (const std::exception& e) {
    BOLT_FAIL("Failed to launch Minio server: {}", e.what());
  }
}

void MinioServer::stop() {
  if (serverProcess_ && serverProcess_->valid()) {
    // Brutal shutdown
    serverProcess_->terminate();
    serverProcess_->wait();
  }
}
