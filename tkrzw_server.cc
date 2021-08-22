/*************************************************************************************************
 * RPC server command of Tkrzw
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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "tkrzw_cmd_util.h"
#include "tkrzw_server_impl.h"

namespace tkrzw {

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_server";
  P("%s: RPC server of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s [options] [db_configs]\n", progname);
  P("\n");
  P("Options:\n");
  P("  --version : Prints the version number and exit.\n");
  P("  --host : The binding address/hostname of the service (default: 0.0.0.0)\n");
  P("  --port : The port number of the service. (default: 1978)\n");
  P("\n");
  P("A database config is in \"path#params\" format.\n");
  P("e.g.: \"casket.tkh#num_buckets=1000000,align_pow=4\"\n");
  P("\n");
  std::exit(1);
}

// Shutdowns the server.
grpc::Server* g_server = nullptr;
void ShutdownServer(int signum) {
  if (g_server != nullptr) {
    PrintL("Shutting down");
    g_server->Shutdown();
    g_server = nullptr;
  }
}

// Processes the command.
static int32_t Process(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--version", 0}, {"--host", 1}, {"--port", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  if (CheckMap(cmd_args, "--version")) {
    PrintL("Tkrzw server ", _TKSERV_PKG_VERSION);
    return 0;
  }
  const std::string host = GetStringArgument(cmd_args, "--host", 0, "0.0.0.0");
  const int32_t port = GetIntegerArgument(cmd_args, "--port", 0, 1978);
  const std::string server_address(StrCat(host, ":", port));
  DBMServiceImpl service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  PrintL("Listening on ", server_address);
  g_server = server.get();
  std::signal(SIGINT, ShutdownServer);
  std::signal(SIGTERM, ShutdownServer);
  std::signal(SIGQUIT, ShutdownServer);
  server->Wait();
  PrintL("Done");
  return 0;
}

}  // namespace tkrzw

// Main routine
int main(int argc, char** argv) {
  const char** args = const_cast<const char**>(argv);
  int32_t rv = 0;
  try {
    rv = tkrzw::Process(argc, args);
  } catch (const std::runtime_error& e) {
    tkrzw::EPrintL(e.what());
    rv = 1;
  }
  return rv;
}

// END OF FILE
