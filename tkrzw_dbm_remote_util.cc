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
  P("  %s clear [options]\n", progname);
  P("    : Removes all records.\n");
  P("  %s rebuild [options] [params]\n", progname);
  P("    : Rebuilds a database file for optimization.\n");
  P("  %s sync [options] [params]\n", progname);
  P("    : Synchronizes a database file.\n");
  P("  %s search [options] pattern\n", progname);
  P("    : Synchronizes a database file.\n");
  P("\n");
  P("Common options:\n");
  P("  --version : Prints the version number and exits.\n");
  P("  --address : The address and the port of the service (default: localhost:1978)\n");
  P("  --timeout : The timeout in seconds for connection and each operation.\n");
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
  std::exit(1);
}

// Processes the echo subcommand.
static int32_t ProcessEcho(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_config = {
    {"--address", 1}, {"--timeout", 1},
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
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the inspect subcommand.
static int32_t ProcessInspect(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the get subcommand.
static int32_t ProcessGet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1}, {"--multi", 0},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  if (!is_multi && cmd_args[""].size() != 1) {
    Die("The key must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the set subcommand.
static int32_t ProcessSet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1}, {"--multi", 0},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  const bool with_no_overwrite = CheckMap(cmd_args, "--no_overwrite");
  const std::string append_delim = GetStringArgument(cmd_args, "--append", 0, "[\xFF|\xFF|\xFF]");
  const int64_t incr_init = GetIntegerArgument(cmd_args, "--incr", 0, INT64MIN);
  if (!is_multi && cmd_args[""].size() != 2) {
    Die("The key and the value must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the remove subcommand.
static int32_t ProcessRemove(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1}, {"--multi", 0},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool is_multi = CheckMap(cmd_args, "--multi");
  if (!is_multi && cmd_args[""].size() != 1) {
    Die("The key must be specified");
  }
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the list subcommand.
static int32_t ProcessList(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--index", 1},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const std::string jump_key = GetStringArgument(cmd_args, "--jump_key", 0, "");
  const std::string move_type = GetStringArgument(cmd_args, "--move", 0, "first");
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, 10);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool keys_only = CheckMap(cmd_args, "--keys");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the clear subcommand.
static int32_t ProcessClear(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--address", 1}, {"--timeout", 1}, {"--index", 1},
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
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the rebuild subcommand.
static int32_t ProcessRebuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the sync subcommand.
static int32_t ProcessSync(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--address", 1}, {"--timeout", 1}, {"--index", 1}, {"--hard", 0},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool with_hard = CheckMap(cmd_args, "--hard");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
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
  dbm.Disconnect();
  return ok ? 0 : 1;
}

// Processes the search subcommand.
static int32_t ProcessSearch(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--address", 1}, {"--timeout", 1}, {"--index", 1},
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
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const std::string mode = GetStringArgument(cmd_args, "--mode", 0, "contain");
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, 10);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool keys_only = CheckMap(cmd_args, "--keys");
  RemoteDBM dbm;
  Status status = dbm.Connect(address, timeout);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  dbm.SetDBMIndex(dbm_index);
  bool ok = false;
  std::vector<std::string> matched;
  status = dbm.SearchModal(mode, pattern, &matched, num_items);
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
  dbm.Disconnect();
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
    } else if (std::strcmp(args[1], "clear") == 0) {
      rv = tkrzw::ProcessClear(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "rebuild") == 0) {
      rv = tkrzw::ProcessRebuild(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "sync") == 0) {
      rv = tkrzw::ProcessSync(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "search") == 0) {
      rv = tkrzw::ProcessSearch(argc - 1, args + 1);
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
