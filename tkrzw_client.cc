/*************************************************************************************************
 * RPC client command of Tkrzw
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
  P("%s: RPC client of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s getversion [options]\n", progname);
  P("  %s inspect [options]\n", progname);
  P("  %s get [options] key\n", progname);
  P("  %s set [options] key value\n", progname);
  P("  %s remove [options] key\n", progname);
  P("  %s clear [options]\n", progname);
  P("  %s rebuild [options] [params]\n", progname);
  P("  %s sync [options] [params]\n", progname);
  P("\n");
  P("Common options:\n");
  P("  --host : The binding address/hostname of the service (default: localhost)\n");
  P("  --port : The port number of the service. (default: 1978)\n");
  P("  --index : The index of the DBM to access. (default: 0)\n");
  P("\n");
  P("Options for the set subcommand:\n");
  P("  --no_overwrite : Fails if there's an existing record wit the same key.\n");
  P("  --append str : Appends the value at the end after the given delimiter.\n");
  P("  --incr num : Increments the value with the given initial value.\n");
  P("\n");
  P("Options for the sync subcommand:\n");
  P("  --hard : Does physical synchronization with the hardware.\n");
  P("\n");
  std::exit(1);
}

// Processes the getversion subcommand.
static int32_t ProcessGetVersion(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--host", 1}, {"--port", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  bool ok = false;
  std::string version;
  status = client.GetVersion(&version);
  if (status == Status::SUCCESS) {
    PrintL(version);
    ok = true;
  } else {
    EPrintL("GetVersion failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the inspect subcommand.
static int32_t ProcessInspect(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--host", 1}, {"--port", 1}, {"--index", 1},
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
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  std::vector<std::pair<std::string, std::string>> records;
  status = client.Inspect(&records);
  if (status == Status::SUCCESS) {
    for (const auto& record : records) {
      PrintL(StrCat(record.first, "=", record.second));
    }
    ok = true;
  } else {
    EPrintL("Inspect failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the get subcommand.
static int32_t ProcessGet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--host", 1}, {"--port", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string key = GetStringArgument(cmd_args, "", 0, "");
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  std::string value;
  status = client.Get(key, &value);
  if (status == Status::SUCCESS) {
    PrintL(value);
    ok = true;
  } else {
    EPrintL("Get failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the set subcommand.
static int32_t ProcessSet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--host", 1}, {"--port", 1}, {"--index", 1},
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
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool with_no_overwrite = CheckMap(cmd_args, "--no_overwrite");
  const std::string append_delim = GetStringArgument(cmd_args, "--append", 0, "[\xFF|\xFF|\xFF]");
  const int64_t incr_init = GetIntegerArgument(cmd_args, "--incr", 0, INT64MIN);
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  if (incr_init != INT64MIN) {
    int64_t current = 0;
    status = client.Increment(key, StrToInt(value), &current, incr_init);
    if (status == Status::SUCCESS) {
      PrintL(current);
      ok = true;
    } else {
      EPrintL("Increment failed: ", status);
    }
  } else if (append_delim != "[\xFF|\xFF|\xFF]") {
    status = client.Append(key, value, append_delim);
    if (status == Status::SUCCESS) {
      ok = true;
    } else {
      EPrintL("Append failed: ", status);
    }
  } else {
    status = client.Set(key, value, !with_no_overwrite);
    if (status == Status::SUCCESS) {
      ok = true;
    } else {
      EPrintL("Set failed: ", status);
    }
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the remove subcommand.
static int32_t ProcessRemove(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--host", 1}, {"--port", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string key = GetStringArgument(cmd_args, "", 0, "");
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  status = client.Remove(key);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the clear subcommand.
static int32_t ProcessClear(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--host", 1}, {"--port", 1}, {"--index", 1},
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
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  status = client.Clear();
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the rebuild subcommand.
static int32_t ProcessRebuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--host", 1}, {"--port", 1}, {"--index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  const std::map<std::string, std::string>& params =
      StrSplitIntoMap(params_expr, ",", "=");
  status = client.Rebuild(params);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  client.Disconnect();
  return ok ? 0 : 1;
}

// Processes the sync subcommand.
static int32_t ProcessSync(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--host", 1}, {"--port", 1}, {"--index", 1},
    {"--hard", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string params_expr = GetStringArgument(cmd_args, "", 0, "");
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--index", 0, 0);
  const bool with_hard = CheckMap(cmd_args, "--hard");
  DBMClient client;
  Status status = client.Connect(host, port);
  if (status != Status::SUCCESS) {
    EPrintL("Connect failed: ", status);
    return 1;
  }
  client.SetDBMIndex(dbm_index);
  bool ok = false;
  const std::map<std::string, std::string>& params =
      StrSplitIntoMap(params_expr, ",", "=");
  status = client.Synchronize(with_hard, params);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  client.Disconnect();
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
    if (std::strcmp(args[1], "getversion") == 0) {
      rv = tkrzw::ProcessGetVersion(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "inspect") == 0) {
      rv = tkrzw::ProcessInspect(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "get") == 0) {
      rv = tkrzw::ProcessGet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "set") == 0) {
      rv = tkrzw::ProcessSet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "remove") == 0) {
      rv = tkrzw::ProcessRemove(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "remove") == 0) {
      rv = tkrzw::ProcessRemove(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "clear") == 0) {
      rv = tkrzw::ProcessClear(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "rebuild") == 0) {
      rv = tkrzw::ProcessRebuild(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "sync") == 0) {
      rv = tkrzw::ProcessSync(argc - 1, args + 1);
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
