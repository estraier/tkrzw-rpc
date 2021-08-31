/*************************************************************************************************
 * Performance checker of RemoteDBM of Tkrzw
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

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm_remote.h"
#include "tkrzw_rpc_common.h"

namespace tkrzw {

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_dbm_remote_perf";
  P("%s: Performance checker of RemoteDBM of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s sequence [options]\n", progname);
  P("    : Checks echoing/setting/getting/removing performance in sequence.\n");
  P("  %s wicked [options]\n", progname);
  P("    : Checks consistency with various operations.\n");
  P("\n");
  P("Common options:\n");
  P("  --address : The address and the port of the service (default: localhost:1978)\n");
  P("  --timeout : The timeout in seconds for connection and each operation.\n");
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
  P("  --iter_only : Does only iterating.\n");
  P("  --remove_only : Does only removing.\n");
  P("\n");
  std::exit(1);
}

// Processes the sequence subcommand.
static int32_t ProcessSequence(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--index", 1},
    {"--iter", 1}, {"--size", 1}, {"--threads", 1},
    {"--random_seed", 1}, {"--random_key", 0}, {"--random_value", 0},
    {"--echo_only", 0}, {"--set_only", 0}, {"--get_only", 0},
    {"--iter_only", 0}, {"--remove_only", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t random_seed = GetIntegerArgument(cmd_args, "--random_seed", 0, 0);
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_random_value = CheckMap(cmd_args, "--random_value");
  bool echo_only = CheckMap(cmd_args, "--echo_only");
  bool set_only = CheckMap(cmd_args, "--set_only");
  bool get_only = CheckMap(cmd_args, "--get_only");
  bool iter_only = CheckMap(cmd_args, "--iter_only");
  bool remove_only = CheckMap(cmd_args, "--remove_only");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  if (!echo_only && !set_only && !get_only && !iter_only && !remove_only) {
    echo_only = true;
    set_only = true;
    get_only = true;
    iter_only = true;
    remove_only = true;
  }
  const int64_t start_mem_rss = GetMemoryUsage();
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
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
      const Status status = dbm.Echo(key, &echo);
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
  if (echo_only) {
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
    const int64_t num_records = dbm.CountSimple();
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
      const Status status = dbm.Set(key, value);
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
  if (set_only) {
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
    const int64_t num_records = dbm.CountSimple();
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
      const Status status = dbm.Get(key, &value);
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
  if (get_only) {
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
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Getting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto iterating_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char key_buf[32];
    bool midline = false;
    std::unique_ptr<tkrzw::RemoteDBM::Iterator> iter;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      if (i % 100 == 0) {
        iter = dbm.MakeIterator();
      }
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const size_t key_size = std::sprintf(key_buf, "%08d", key_num);
      const std::string_view key(key_buf, key_size);
      Status status = iter->Jump(key);
      if (status != Status::SUCCESS &&
          !(is_random_key && random_seed < 0 && status == Status::NOT_FOUND_ERROR)) {
        EPrintL("Jump failed: ", status);
        has_error = true;
        break;
      }
      std::string rec_key, rec_value;
      status = iter->Get(&rec_key, &rec_value);
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
  if (iter_only) {
    PrintF("Iterating: num_iterations=%d value_size=%d num_threads=%d\n",
           num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(iterating_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Iterating done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
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
      const Status status = dbm.Remove(key);
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
  if (remove_only) {
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
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Removing done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  return has_error ? 1 : 0;
}

// Processes the wicked subcommand.
static int32_t ProcessWicked(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--index", 1},
    {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--random_seed", 1},
    {"--iterator", 0}, {"--sync", 0}, {"--clear", 0}, {"--rebuild", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t random_seed = GetIntegerArgument(cmd_args, "--random_seed", 0, 0);
  const bool with_iterator = CheckMap(cmd_args, "--iterator");
  const bool with_sync = CheckMap(cmd_args, "--sync");
  const bool with_clear = CheckMap(cmd_args, "--clear");
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  std::atomic_bool has_error(false);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 key_mt(mt_seed + id);
    std::mt19937 misc_mt(mt_seed * 2 + id + 1);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    char* value_buf = new char[value_size];
    std::memset(value_buf, '0' + id % 10, value_size);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = key_num_dist(key_mt);
      const std::string& key = SPrintF("%08d", key_num);
      std::string_view value(value_buf, value_size_dist(misc_mt));
      if (with_rebuild && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm.Rebuild();
        if (status != Status::SUCCESS) {
          EPrintL("Rebuild failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_clear && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm.Clear();
        if (status != Status::SUCCESS) {
          EPrintL("Clear failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_sync && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm.Synchronize(false);
        if (status != Status::SUCCESS) {
          EPrintL("Synchronize failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_iterator && op_dist(misc_mt) % 100 == 0) {
        auto iter = dbm.MakeIterator();
        if (op_dist(misc_mt) % 2 == 0) {
          const Status status = iter->First();
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Iterator::First failed: ", status);
            has_error = true;
            break;
          }
        } else {
          const Status status = iter->Jump(key);
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Iterator::Jump failed: ", status);
            has_error = true;
            break;
          }
        }
        for (int32_t j = 0; j < 3; ++j) {
          if (op_dist(misc_mt) % 5 == 0) {
            const Status status = iter->Remove();
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Remove failed: ", status);
                has_error = true;
              }
              break;
            }
          } else if (op_dist(misc_mt) % 3 == 0) {
            const Status status = iter->Set(value);
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Set failed: ", status);
                has_error = true;
              }
              break;
            }
          } else {
            std::string tmp_key, tmp_value;
            const Status status = iter->Get(&tmp_key, &tmp_value);
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Get failed: ", status);
                has_error = true;
              }
              break;
            }
          }
          const Status status = iter->Next();
          if (status != Status::SUCCESS) {
            if (status != Status::NOT_FOUND_ERROR) {
              EPrintL("Iterator::Next failed: ", status);
              has_error = true;
              }
            break;
          }
        }
      } else if (op_dist(misc_mt) % 8 == 0) {
        const Status status = dbm.Append(key, value, ",");
        if (status != Status::SUCCESS) {
          EPrintL("Append failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(misc_mt) % 5 == 0) {
        const Status status = dbm.Remove(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Remove failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(misc_mt) % 3 == 0) {
        const bool overwrite = op_dist(misc_mt) % 3 != 0;
        const Status status = dbm.Set(key, value, overwrite);
        if (status != Status::SUCCESS && status != Status::DUPLICATION_ERROR) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      } else {
        std::string rec_value;
        const Status status = dbm.Get(key, &rec_value);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Get failed: ", status);
          has_error = true;
          break;
        }
      }
      if (id == 0 && i == num_iterations / 2) {
        const Status status = dbm.Synchronize(false);
        if (status != Status::SUCCESS) {
          EPrintL("Synchronize failed: ", status);
          has_error = true;
          break;
        }
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
  PrintF("Doing: num_iterations=%d value_size=%d num_threads=%d\n",
         num_iterations, value_size, num_threads);
  const double start_time = GetWallTime();
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  PrintL();
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
    } else if (std::strcmp(args[1], "wicked") == 0) {
      rv = tkrzw::ProcessWicked(argc - 1, args + 1);
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
