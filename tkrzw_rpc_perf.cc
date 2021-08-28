/*************************************************************************************************
 * Performance checker of Tkrzw-RPC
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

#include <cassert>
#include <cstdarg>
#include <cstdint>

#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "tkrzw_rpc.h"
#include "tkrzw_cmd_util.h"

namespace tkrzw {

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_client";
  P("%s: Performance checker of Tkrzw-RPC\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s sequence [options]\n", progname);
  P("    : Checks echoing/setting/getting/removing performance in sequence.\n");
  P("\n");
  P("Common options:\n");
  P("  --host : The binding address/hostname of the service (default: localhost)\n");
  P("  --port : The port number of the service. (default: 1978)\n");
  P("  --index : The index of the DBM to access. (default: 0)\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --size num : The size of each record value. (default: 8)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --random_seed num : The random seed or negative for real RNG. (default: 0)\n");
  P("\n");
  P("Options for the sequence subcommand:\n");
  P("  --random_key : Uses random keys rather than sequential ones.\n");
  P("  --random_value : Uses random length values rather than fixed ones.\n");
  P("  --echo_only : Does only echoing.\n");
  P("  --set_only : Does only setting.\n");
  P("  --get_only : Does only getting.\n");
  P("  --remove_only : Does only removing.\n");
  P("\n");
  std::exit(1);
}

// Processes the sequence subcommand.
static int32_t ProcessSequence(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--host", 1}, {"--port", 1}, {"--index", 1},
    {"--iter", 1}, {"--size", 1}, {"--threads", 1},
    {"--random_seed", 1}, {"--random_key", 0}, {"--random_value", 0},
    {"--echo_only", 0}, {"--set_only", 0}, {"--get_only", 0}, {"--remove_only", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t random_seed = GetIntegerArgument(cmd_args, "--random_seed", 0, 0);
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_random_value = CheckMap(cmd_args, "--random_value");
  const bool is_echo_only = CheckMap(cmd_args, "--echo_only");
  const bool is_set_only = CheckMap(cmd_args, "--set_only");
  const bool is_get_only = CheckMap(cmd_args, "--get_only");
  const bool is_remove_only = CheckMap(cmd_args, "--remove_only");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  const int64_t start_mem_rss = GetMemoryUsage();
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  std::atomic_bool has_error(false);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto echoing_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::mt19937 misc_mt(mt_seed * 2 + id + 1);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char key_buf[32];
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const size_t key_size = std::sprintf(key_buf, "%08d", key_num);
      const std::string_view key(key_buf, key_size);
      std::string echo;
      const Status status = client.Echo(key, &echo);
      if (status != Status::SUCCESS) {
        EPrintL("Echo failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  if (!is_set_only && !is_get_only && !is_remove_only) {
    PrintF("Echoing: num_iterations=%d value_size=%d num_threads=%d\n",
           num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(echoing_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = client.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Echoing done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto setting_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::mt19937 misc_mt(mt_seed * 2 + id + 1);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char key_buf[32];
    char* value_buf = new char[value_size];
    std::memset(value_buf, '0' + id % 10, value_size);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const size_t key_size = std::sprintf(key_buf, "%08d", key_num);
      const std::string_view key(key_buf, key_size);
      const std::string_view value(
          value_buf, is_random_value ? value_size_dist(misc_mt) : value_size);
      const Status status = client.Set(key, value);
      if (status != Status::SUCCESS) {
        EPrintL("Set failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
    delete[] value_buf;
  };
  if (!is_echo_only && !is_get_only && !is_remove_only) {
    PrintF("Setting: num_iterations=%d value_size=%d num_threads=%d\n",
           num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(setting_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = client.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Setting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto getting_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char key_buf[32];
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const size_t key_size = std::sprintf(key_buf, "%08d", key_num);
      const std::string_view key(key_buf, key_size);
      std::string value;
      const Status status = client.Get(key, &value);
      if (status != Status::SUCCESS &&
          !(is_random_key && random_seed < 0 && status == Status::NOT_FOUND_ERROR)) {
        EPrintL("Get failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  if (!is_echo_only && !is_set_only && !is_remove_only) {
    PrintF("Getting: num_iterations=%d value_size=%d num_threads=%d\n",
           num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(getting_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = client.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Getting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto removing_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char key_buf[32];
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const size_t key_size = std::sprintf(key_buf, "%08d", key_num);
      const std::string_view key(key_buf, key_size);
      const Status status = client.Remove(key);
      if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
        EPrintL("Remove failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  if (!is_echo_only && !is_set_only && !is_get_only) {
    PrintF("Removing: num_iterations=%d value_size=%d num_threads=%d\n",
           num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(removing_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = client.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Removing done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  return has_error ? 1 : 0;
}

}  // namespace tkrzw

// Main routine
int main(int argc, char** argv) {
  const char** args = const_cast<const char**>(argv);
  if (argc < 2) {
    tkrzw::PrintUsageAndDie();
  }
  int32_t rv = 0;
  try {
    if (std::strcmp(args[1], "sequence") == 0) {
      rv = tkrzw::ProcessSequence(argc - 1, args + 1);
    } else {
      tkrzw::PrintUsageAndDie();
    }
  } catch (const std::runtime_error& e) {
    tkrzw::EPrintL(e.what());
    rv = 1;
  }
  return rv;
}

// END OF FILE
