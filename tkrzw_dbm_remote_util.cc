/*************************************************************************************************
 * Command line interface of RemoteDBM utilities
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
#include <csignal>
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
  const char* progname = "tkrzw_dbm_remote_util";
  P("%s: RemoteDBM utilities of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s echo [options] [message]\n", progname);
  P("    : Invokes an echoing back test.\n");
  P("  %s inspect [options] [attr]\n", progname);
  P("    : Prints inspection of a database file.\n");
  P("  %s get [options] key\n", progname);
  P("    : Gets a record and prints it.\n");
  P("  %s set [options] key value\n", progname);
  P("    : Sets a record.\n");
  P("  %s remove [options] key\n", progname);
  P("    : Removes a record\n");
  P("  %s list [options]\n", progname);
  P("    : Lists up records and prints them.\n");
  P("  %s queue [options] [value]\n", progname);
  P("    : Enqueue or dequeue a record.\n");
  P("  %s clear [options]\n", progname);
  P("    : Removes all records.\n");
  P("  %s rebuild [options] [params]\n", progname);
  P("    : Rebuilds a database file for optimization.\n");
  P("  %s sync [options] [params]\n", progname);
  P("    : Synchronizes a database file.\n");
  P("  %s search [options] pattern\n", progname);
  P("    : Synchronizes a database file.\n");
  P("  %s changemaster [options] [master]\n", progname);
  P("    : Changes the master of replication.\n");
  P("  %s replicate [options] [db_configs...]\n", progname);
  P("    : Replicates updates to local databases.\n");
  P("\n");
  P("Common options:\n");
  P("  --version : Prints the version number and exits.\n");
  P("  --address : The address and the port of the service (default: localhost:1978)\n");
  P("  --timeout : The timeout in seconds for connection and each operation.\n");
  P("  --auth configs : Enables authentication with the configuration.\n");
  P("  --index : The index of the DBM to access. (default: 0)\n");
  P("  --multi : Calls xxxMulti methods for get, set, and remove subcommands.\n");
  P("\n");
  P("Options for the set subcommand:\n");
  P("  --no_overwrite : Fails if there's an existing record wit the same key.\n");
  P("  --append str : Appends the value at the end after the given delimiter.\n");
  P("  --incr num : Increments the value with the given initial value.\n");
  P("\n");
  P("Options for the list subcommand:\n");
  P("  --move type : Type of movement:"
    " first, jump, jumplower, jumplowerinc, jumpupper, jumpupperinc. (default: first)\n");
  P("  --jump_key str : Specifies the jump key. (default: empty string)\n");
  P("  --items num : The number of items to print. (default: 10)\n");
  P("  --escape : C-style escape is applied to the TSV data.\n");
  P("  --keys : Prints keys only.\n");
  P("\n");
  P("Options for the queue subcommand:\n");
  P("  --notify : Sends notifications when queueing.\n");
  P("  --retry num : The maximum wait time in seconds before retrying.\n");
  P("  --escape : C-style escape is applied to the output data.\n");
  P("  --key : Prints the key too.\n");
  P("  --compex str : Calls CompareExchange with the key.\n");
  P("\n");
  P("Options for the sync subcommand:\n");
  P("  --hard : Does physical synchronization with the hardware.\n");
  P("\n");
  P("Options for the search subcommand:\n");
  P("  --mode str : The search mode:"
    " contain, begin, end, regex, edit, editbin, upper, upperinc, lower, lowerinc"
    " (default: contain)\n");
  P("  --items num : The number of items to retrieve. (default: 10)\n");
  P("  --escape : C-style escape is applied to the TSV data.\n");
  P("  --keys : Prints keys only.\n");
  P("\n");
  P("Options for the changemaster subcommand:\n");
  P("  --ts_skew num : Skews the timestamp by a value.\n");
  P("\n");
  P("Options for the replication subcommand:\n");
  P("  --ts_file str : The replication timestamp file.\n");
  P("  --ts_from_dbm : Uses the database timestamp if the timestamp file doesn't exist.\n");
  P("  --ts_skew num : Skews the timestamp by a value.\n");
  P("  --server_id num : The server ID of the client. (default: 0)\n");
  P("  --wait num : The time in seconds to wait for the next log. (default: 1)\n");
  P("  --items num : The number of items to print. (default: unlimited)\n");
  P("  --escape : C-style escape is applied to the TSV data.\n");
  P("\n");
  std::exit(1);
}

// Shutdowns the process.
bool g_process_alive = true;
void ShutdownProcess(int signum) {
  g_process_alive = false;
}

// Processes the echo subcommand.
static int32_t ProcessEcho(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_config = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_config, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string message = StrJoin(cmd_args[""], " ");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  bool ok = false;
  std::string echo;
  status = dbm.Echo(message, &echo);
  if (status == Status::SUCCESS) {
    PrintL(echo);
    ok = true;
  } else {
    EPrintL("Echo failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the inspect subcommand.
static int32_t ProcessInspect(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string attr_name = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  std::vector<std::pair<std::string, std::string>> records;
  status = dbm.Inspect(&records);
  if (status == Status::SUCCESS) {
    if (attr_name.empty()) {
      for (const auto& record : records) {
        PrintL(StrCat(record.first, "=", record.second));
      }
    } else {
      for (const auto& record : records) {
        if (record.first == attr_name) {
          PrintL(record.second);
        }
      }
    }
    ok = true;
  } else {
    EPrintL("Inspect failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the get subcommand.
static int32_t ProcessGet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1}, {"--multi", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string key = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  if (!is_multi && cmd_args[""].size() != 1) {
    Die("The key must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  if (is_multi) {
    std::vector<std::string_view> keys;
    const auto& rec_args = cmd_args[""];
    for (int32_t i = 0; i < static_cast<int32_t>(rec_args.size()); i++) {
      keys.emplace_back(rec_args[i]);
    }
    std::map<std::string, std::string> records;
    const Status status = dbm.GetMulti(keys, &records);
    if (status == Status::SUCCESS || status == Status::NOT_FOUND_ERROR) {
      for (const auto& record : records) {
        PrintL(record.first, "\t", record.second);
      }
      ok = true;
    } else {
      EPrintL("GetMulti failed: ", status);
    }
  } else {
    std::string value;
    status = dbm.Get(key, &value);
    if (status == Status::SUCCESS) {
      PrintL(value);
      ok = true;
    } else {
      EPrintL("Get failed: ", status);
    }
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the set subcommand.
static int32_t ProcessSet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1}, {"--multi", 0},
    {"--no_overwrite", 0}, {"--append", 1}, {"--incr", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string key = GetStringArgument(cmd_args, "", 0, "");
  const std::string value = GetStringArgument(cmd_args, "", 1, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  const bool with_no_overwrite = CheckMap(cmd_args, "--no_overwrite");
  const std::string append_delim = GetStringArgument(cmd_args, "--append", 0, "[\xFF|\xFF|\xFF]");
  const int64_t incr_init = GetIntegerArgument(cmd_args, "--incr", 0, INT64MIN);
  if (!is_multi && cmd_args[""].size() != 2) {
    Die("The key and the value must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  if (incr_init != INT64MIN) {
    int64_t current = 0;
    status = dbm.Increment(key, StrToInt(value), &current, incr_init);
    if (status == Status::SUCCESS) {
      PrintL(current);
      ok = true;
    } else {
      EPrintL("Increment failed: ", status);
    }
  } else if (append_delim != "[\xFF|\xFF|\xFF]") {
    if (is_multi) {
      std::map<std::string_view, std::string_view> records;
      const auto& rec_args = cmd_args[""];
      for (int32_t i = 0; i < static_cast<int32_t>(rec_args.size()) - 1; i += 2) {
        records.emplace(rec_args[i], rec_args[i + 1]);
      }
      const Status status = dbm.AppendMulti(records, append_delim);
      if (status == Status::SUCCESS) {
        ok = true;
      } else {
        EPrintL("AppendMulti failed: ", status);
      }
    } else {
      status = dbm.Append(key, value, append_delim);
      if (status == Status::SUCCESS) {
        ok = true;
      } else {
        EPrintL("Append failed: ", status);
      }
    }
  } else {
    if (is_multi) {
      std::map<std::string_view, std::string_view> records;
      const auto& rec_args = cmd_args[""];
      for (int32_t i = 0; i < static_cast<int32_t>(rec_args.size()) - 1; i += 2) {
        records.emplace(rec_args[i], rec_args[i + 1]);
      }
      const Status status = dbm.SetMulti(records, !with_no_overwrite);
      if (status == Status::SUCCESS) {
        ok = true;
      } else {
        EPrintL("SetMulti failed: ", status);
      }
    } else {
      status = dbm.Set(key, value, !with_no_overwrite);
      if (status == Status::SUCCESS) {
        ok = true;
      } else {
        EPrintL("Set failed: ", status);
      }
    }
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the remove subcommand.
static int32_t ProcessRemove(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1}, {"--multi", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string key = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  if (!is_multi && cmd_args[""].size() != 1) {
    Die("The key must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  if (is_multi) {
    std::vector<std::string_view> keys;
    const auto& rec_args = cmd_args[""];
    for (int32_t i = 0; i < static_cast<int32_t>(rec_args.size()); i++) {
      keys.emplace_back(rec_args[i]);
    }
    const Status status = dbm.RemoveMulti(keys);
    if (status == Status::SUCCESS || status == Status::NOT_FOUND_ERROR) {
      ok = true;
    } else {
      EPrintL("RemoveMulti failed: ", status);
    }
  } else {
    status = dbm.Remove(key);
    if (status == Status::SUCCESS) {
      ok = true;
    } else {
      EPrintL("Remove failed: ", status);
    }
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the list subcommand.
static int32_t ProcessList(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
    {"--move", 1}, {"--jump_key", 1}, {"--items", 1}, {"--escape", 0}, {"--keys", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const std::string jump_key = GetStringArgument(cmd_args, "--jump_key", 0, "");
  const std::string move_type = GetStringArgument(cmd_args, "--move", 0, "first");
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, 10);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool keys_only = CheckMap(cmd_args, "--keys");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = true;
  auto iter = dbm.MakeIterator();
  bool forward = true;
  if (move_type == "jump") {
    const Status status = iter->Jump(jump_key);
    if (status != Status::SUCCESS) {
      EPrintL("Jump failed: ", status);
      ok = false;
    }
  } else if (move_type == "jumplower") {
    const Status status = iter->JumpLower(jump_key, false);
    if (status != Status::SUCCESS) {
      EPrintL("JumpLower failed: ", status);
      ok = false;
    }
  } else if (move_type == "jumplowerinc") {
    const Status status = iter->JumpLower(jump_key, true);
    if (status != Status::SUCCESS) {
      EPrintL("JumpLower failed: ", status);
      ok = false;
    }
  } else if (move_type == "jumpupper") {
    const Status status = iter->JumpUpper(jump_key, false);
    if (status != Status::SUCCESS) {
      EPrintL("JumpUpper failed: ", status);
      ok = false;
    }
    forward = false;
  } else if (move_type == "jumpupperinc") {
    const Status status = iter->JumpUpper(jump_key, true);
    if (status != Status::SUCCESS) {
      EPrintL("JumpUpper failed: ", status);
      ok = false;
    }
    forward = false;
  } else {
    const Status status = iter->First();
    if (status != Status::SUCCESS) {
      EPrintL("First failed: ", status);
      ok = false;
    }
  }
  for (int64_t count = 0; ok && count < num_items; count++) {
    std::string key, value;
    Status status = iter->Get(&key, keys_only ? nullptr : &value);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        EPrintL("Get failed: ", status);
        ok = false;
      }
      break;
    }
    if (keys_only) {
      const std::string& esc_key = with_escape ? StrEscapeC(key) : StrTrimForTSV(key);
      PrintL(esc_key);
    } else {
      const std::string& esc_key = with_escape ? StrEscapeC(key) : StrTrimForTSV(key);
      const std::string& esc_value = with_escape ? StrEscapeC(value) : StrTrimForTSV(value, true);
      PrintL(esc_key, "\t", esc_value);
    }
    if (forward) {
      status = iter->Next();
      if (status != Status::SUCCESS) {
        EPrintL("Next failed: ", status);
        ok = false;
        break;
      }
    } else {
      status = iter->Previous();
      if (status != Status::SUCCESS) {
        EPrintL("Previous failed: ", status);
        ok = false;
        break;
      }
    }
  }
  iter.reset(nullptr);
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the queue subcommand.
static int32_t ProcessQueue(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
    {"--notify", 0}, {"--retry", 1}, {"--escape", 0}, {"--key", 0}, {"--compex", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool notify = CheckMap(cmd_args, "--notify");
  const double retry_wait = GetDoubleArgument(cmd_args, "--retry", 0, 0);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool key_also = CheckMap(cmd_args, "--key");
  const std::string compex_key =
      GetStringArgument(cmd_args, "--compex", 0, SkipDBM::REMOVING_VALUE);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = true;
  if (compex_key != SkipDBM::REMOVING_VALUE) {
    if (cmd_args[""].empty()) {
      std::string value;
      status = dbm.CompareExchange(
          compex_key, DBM::ANY_DATA, std::string_view(), &value, nullptr, retry_wait, notify);
      if (status == Status::SUCCESS) {
        const std::string& esc_key =
            with_escape ? StrEscapeC(compex_key) : StrTrimForTSV(compex_key, true);
        const std::string& esc_value =
            with_escape ? StrEscapeC(value) : StrTrimForTSV(value, true);
        if (key_also) {
          PrintL(esc_key, "\t", esc_value);
        } else {
          PrintL(esc_value);
        }
      } else {
        EPrintL("CompareExchange failed: ", status);
        ok = false;
      }
    } else {
      for (const auto& value : cmd_args[""]) {
        status = dbm.CompareExchange(
            compex_key, std::string_view(), value, nullptr, nullptr, retry_wait, notify);
        if (status != Status::SUCCESS) {
          EPrintL("CompareExchange failed: ", status);
          ok = false;
          break;
        }
      }
    }
  } else {
    if (cmd_args[""].empty()) {
      std::string key, value;
      status = dbm.PopFirst(&key, &value, retry_wait);
      if (status == Status::SUCCESS) {
        const std::string& esc_key = SPrintF("%010.6f", StrToIntBigEndian(key) / 100000000.0);
        const std::string& esc_value =
            with_escape ? StrEscapeC(value) : StrTrimForTSV(value, true);
        if (key_also) {
          PrintL(esc_key, "\t", esc_value);
        } else {
          PrintL(esc_value);
        }
      } else {
        EPrintL("PopFirst failed: ", status);
        ok = false;
      }
    } else {
      for (const auto& value : cmd_args[""]) {
        status = dbm.PushLast(value, -1, notify);
        if (status != Status::SUCCESS) {
          EPrintL("PushLast failed: ", status);
          ok = false;
          break;
        }
      }
    }
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the clear subcommand.
static int32_t ProcessClear(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  status = dbm.Clear();
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the rebuild subcommand.
static int32_t ProcessRebuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  const std::map<std::string, std::string>& params =
      StrSplitIntoMap(params_expr, ",", "=");
  status = dbm.Rebuild(params);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the sync subcommand.
static int32_t ProcessSync(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1}, {"--hard", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool with_hard = CheckMap(cmd_args, "--hard");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  const std::map<std::string, std::string>& params =
      StrSplitIntoMap(params_expr, ",", "=");
  status = dbm.Synchronize(with_hard, params);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the search subcommand.
static int32_t ProcessSearch(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
    {"--mode", 1}, {"--items", 1}, {"--escape", 0}, {"--keys", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string pattern = GetStringArgument(cmd_args, "", 0, "");
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const std::string mode = GetStringArgument(cmd_args, "--mode", 0, "contain");
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, 10);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool keys_only = CheckMap(cmd_args, "--keys");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  std::vector<std::string> matched;
  status = dbm.Search(mode, pattern, &matched, num_items);
  if (status == Status::SUCCESS) {
    if (keys_only) {
      for (const auto& key : matched) {
        const std::string& esc_key = with_escape ? StrEscapeC(key) : StrTrimForTSV(key);
        PrintL(esc_key);
      }
    } else {
      auto printer =
          [&](const std::vector<std::string_view>& keys) {
            std::map<std::string, std::string> records;
            status = dbm.GetMulti(keys, &records);
            if (status == Status::SUCCESS || status == Status::NOT_FOUND_ERROR) {
              for (const auto& key : keys) {
                const auto& value = records[std::string(key)];
                const std::string& esc_key = with_escape ? StrEscapeC(key) : StrTrimForTSV(key);
                const std::string& esc_value =
                    with_escape ? StrEscapeC(value) : StrTrimForTSV(value, true);
                PrintL(esc_key, "\t", esc_value);
              }
            } else {
              EPrintL("GetMulti failed: ", status);
            }
          };
      std::vector<std::string_view> keys;
      for (const auto& key : matched) {
        keys.emplace_back(std::string_view(key));
        if (keys.size() >= 100) {
          printer(keys);
          keys.clear();
        }
      }
      if (!keys.empty()) {
        printer(keys);
      }
    }
    ok = true;
  } else {
    EPrintL("Search failed: ", status);
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the changemaster subcommand.
static int32_t ProcessChangeMaster(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--ts_skew", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string master = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int64_t ts_skew = GetIntegerArgument(cmd_args, "--ts_skew", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  bool ok = true;
  status = dbm.ChangeMaster(master, ts_skew);
  if (status != Status::SUCCESS) {
    EPrintL("ChangeMaster failed: ", status);
    ok = false;
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
}

// Processes the replicate subcommand.
static int32_t ProcessReplicate(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--auth", 1}, {"--index", 1},
    {"--ts_file", 1}, {"--ts_from_dbm", 0},{"--ts_skew", 1},
    {"--server_id", 1}, {"--wait", 1},
    {"--items", 1}, {"--escape", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string pattern = GetStringArgument(cmd_args, "", 0, "");
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "localhost:1978");
  const double timeout = GetDoubleArgument(cmd_args, "--timeout", 0, -1);
  const std::string auth_config = GetStringArgument(cmd_args, "--auth", 0, "");
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, -1);
  const std::string ts_file = GetStringArgument(cmd_args, "--ts_file", 0, "");
  const bool ts_from_dbm = CheckMap(cmd_args, "--ts_from_dbm");
  const int64_t ts_skew = GetIntegerArgument(cmd_args, "--ts_skew", 0, 0);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, 0);
  const double wait_time = GetDoubleArgument(cmd_args, "--wait", 0, 1.0);
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, INT64MAX);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const auto& dbm_exprs = cmd_args[""];
  std::vector<std::unique_ptr<DBM>> local_dbms;
  local_dbms.reserve(dbm_exprs.size());
  for (const auto& dbm_expr : dbm_exprs) {
    PrintL("Opening DBM: ", dbm_expr);
    const std::vector<std::string> fields = StrSplit(dbm_expr, "#");
    const std::string path = fields.front();
    std::map<std::string, std::string> params;
    if (fields.size() > 1) {
      params = StrSplitIntoMap(fields[1], ",", "=");
    }
    const int32_t num_shards = StrToInt(SearchMap(params, "num_shards", "-1"));
    std::unique_ptr<ParamDBM> dbm;
    if (num_shards >= 0) {
      dbm = std::make_unique<ShardDBM>();
    } else {
      dbm = std::make_unique<PolyDBM>();
    }
    const Status status = dbm->OpenAdvanced(path, true, File::OPEN_DEFAULT, params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      return 1;
    }
    local_dbms.emplace_back(std::move(dbm));
  }
  int64_t min_timestamp = -1;
  if (!ts_file.empty()) {
    const std::string tsexpr = ReadFileSimple(ts_file, "", 32);
    if (!tsexpr.empty()) {
      min_timestamp = StrToInt(tsexpr);
    }
  }
  if (min_timestamp < 0 && ts_from_dbm) {
    min_timestamp = GetWallTime() * 1000;
    for (const auto& local_dbm : local_dbms) {
      const int64_t dbm_timestamp = local_dbm->GetTimestampSimple() * 1000;
      min_timestamp = std::min<int64_t>(min_timestamp, dbm_timestamp);
    }
  }
  min_timestamp = std::max<int64_t>(0, min_timestamp + ts_skew);
  if (!local_dbms.empty()) {
    PrintL("Using the minimum timestamp: ", min_timestamp);
  }
  if (!local_dbms.empty()) {
    PrintL("Connecting to the server: ", address);
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout, auth_config);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  bool ok = false;
  std::signal(SIGHUP, ShutdownProcess);
  std::signal(SIGINT, ShutdownProcess);
  std::signal(SIGTERM, ShutdownProcess);
  std::signal(SIGQUIT, ShutdownProcess);
  auto repl = dbm.MakeReplicator();
  status = repl->Start(min_timestamp, server_id, wait_time);
  if (status != Status::SUCCESS) {
    EPrintL("Start failed: ", status);
    return 1;
  }
  if (!local_dbms.empty()) {
    PrintL("Doing replication: master_server_id=", repl->GetMasterServerID());
  }
  RemoteDBM::ReplicateLog op;
  int64_t count = 0;
  int64_t count_ops_set = 0;
  int64_t count_ops_remove = 0;
  int64_t count_ops_clear = 0;
  int64_t max_timestamp = -1;
  const int64_t mod_num_items = num_items > 0 ? num_items : INT64MAX;
  bool first = true;
  while (g_process_alive && count < mod_num_items) {
    int64_t timestamp = 0;
    status = repl->Read(&timestamp, &op);
    if (first && !local_dbms.empty()) {
      const double diff = GetWallTime() - timestamp / 1000.0;
      std::string ts_expr = MakeRelativeTimeExpr(diff);
      PrintL("First: timestamp=", timestamp, " (", ts_expr, ")");
    }
    first = false;
    if (status == Status::SUCCESS) {
      max_timestamp = std::max(timestamp, max_timestamp);
      if (dbm_index >= 0 && op.dbm_index != dbm_index) {
        continue;
      }
      if (local_dbms.empty()) {
        const std::string& esc_key = with_escape ? StrEscapeC(op.key) : StrTrimForTSV(op.key);
        const std::string& esc_value =
            with_escape ? StrEscapeC(op.value) : StrTrimForTSV(op.value, true);
        switch (op.op_type) {
          case DBMUpdateLoggerMQ::OP_SET:
            PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index,
                   "\tSET\t", esc_key, "\t", esc_value);
            break;
          case DBMUpdateLoggerMQ::OP_REMOVE:
            PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index, "\tREMOVE\t", esc_key);
            break;
          case DBMUpdateLoggerMQ::OP_CLEAR:
            PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index, "\tCLEAR");
            break;
          default:
            break;
        }
      } else if (op.dbm_index < 0 || op.dbm_index >= static_cast<int32_t>(local_dbms.size())) {
        EPrintL("DBM index is out of range: ", op.dbm_index);
        ok = false;
      } else {
        DBM* local_dbm = local_dbms[op.dbm_index].get();
        switch (op.op_type) {
          case DBMUpdateLoggerMQ::OP_SET:
            count_ops_set++;
            status = local_dbm->Set(op.key, op.value);
            if (status != Status::SUCCESS) {
              EPrintL("Set failed: ", status);
              ok = false;
            }
            break;
          case DBMUpdateLoggerMQ::OP_REMOVE:
            count_ops_remove++;
            status = local_dbm->Remove(op.key);
            if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
              EPrintL("Remove failed: ", status);
              ok = false;
            }
            break;
          case DBMUpdateLoggerMQ::OP_CLEAR:
            count_ops_clear++;
            status = local_dbm->Clear();
            if (status != Status::SUCCESS) {
              EPrintL("Clear failed: ", status);
              ok = false;
            }
            break;
          default:
            break;
        }
      }
      if (!ts_file.empty() && count % 1000 == 0) {
        status = WriteFileAtomic(ts_file, ToString(max_timestamp) + "\n");
        if (status != Status::SUCCESS) {
          EPrintL("WriteFile failed: ", status);
          ok = false;
          break;
        }
      }
      count++;
    } else if (status == Status::INFEASIBLE_ERROR) {
      max_timestamp = std::max(timestamp, max_timestamp);
      if (num_items <= 0) {
        break;
      }
    } else {
      EPrintL("Read failed: ", status);
      ok = false;
      break;
    }
  }
  repl.reset(nullptr);
  if (!local_dbms.empty()) {
    PrintL("Done: timestamp=", max_timestamp, ", set=", count_ops_set,
           ", remove=", count_ops_remove,  ", clear=", count_ops_clear);
  }
  if (!ts_file.empty() && max_timestamp >= 0) {
    status = WriteFileAtomic(ts_file, ToString(max_timestamp) + "\n");
    if (status != Status::SUCCESS) {
      EPrintL("WriteFile failed: ", status);
      ok = false;
    }
  }
  status = dbm.Disconnect();
  if (status != Status::SUCCESS) {
    EPrintL("Disconnect failed: ", status);
    ok = false;
  }
  return ok ? 0 : 1;
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
    if (std::strcmp(args[1], "--version") == 0) {
      tkrzw::PrintL("Tkrzw-RPC client utilities ", tkrzw::RPC_PACKAGE_VERSION);
    } else if (std::strcmp(args[1], "echo") == 0) {
      rv = tkrzw::ProcessEcho(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "inspect") == 0) {
      rv = tkrzw::ProcessInspect(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "get") == 0) {
      rv = tkrzw::ProcessGet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "set") == 0) {
      rv = tkrzw::ProcessSet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "remove") == 0) {
      rv = tkrzw::ProcessRemove(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "list") == 0) {
      rv = tkrzw::ProcessList(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "queue") == 0) {
      rv = tkrzw::ProcessQueue(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "clear") == 0) {
      rv = tkrzw::ProcessClear(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "rebuild") == 0) {
      rv = tkrzw::ProcessRebuild(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "sync") == 0) {
      rv = tkrzw::ProcessSync(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "search") == 0) {
      rv = tkrzw::ProcessSearch(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "changemaster") == 0) {
      rv = tkrzw::ProcessChangeMaster(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "replicate") == 0) {
      rv = tkrzw::ProcessReplicate(argc - 1, args + 1);
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
