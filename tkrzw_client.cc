/*************************************************************************************************
 * Tkrzw server
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
  P("\n");
  P("Common options:\n");
  P("  --host : The binding address/hostname of the service (default: localhost)\n");
  P("  --port : The port number of the service. (default: 1978)\n");
  P("\n");
  std::exit(1);
}

// Processes the getversion subcommand.
static int32_t ProcessGetVersion(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--host", 1}, {"--port", 1},
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
  std::string version;
  status = client.GetVersion(&version);
  if (status != Status::SUCCESS) {
    EPrintL("GetVersion failed: ", status);
    return 1;
  }
  PrintL(version);
  client.Disconnect();
  return 0;
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
