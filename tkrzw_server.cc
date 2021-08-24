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

#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "tkrzw_cmd_util.h"
#include "tkrzw_rpc.h"
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
  P("  --log_file : The file path of the log file. (default: /dev/stdout)\n");
  P("  --log_level : The minimum log level to be stored:"
    " debug, info, warn, error, fatal. (default: info)\n");
  P("  --pid_file : The file path of the store the process ID.\n");
  P("  --daemon : Runs the process as a daemon process.\n");
  P("  --read_only : Opens the databases in the read-only mode.\n");
  P("\n");
  P("A database config is in \"path#params\" format.\n");
  P("e.g.: \"casket.tkh#num_buckets=1000000,align_pow=4\"\n");
  P("\n");
  std::exit(1);
}

// Global variables.
StreamLogger* g_logger = nullptr;
std::string_view g_log_file;
std::string_view g_log_level;
std::ofstream* g_log_stream = nullptr;
std::atomic<grpc::Server*> g_server(nullptr);

// Configures the logger.
Status ConfigLogger() {
  if (g_log_stream->is_open()) {
    g_log_stream->close();
    if (!g_log_stream->good()) {
      return Status(Status::SYSTEM_ERROR, "log close failed");
    }
  }
  if (g_log_file.empty()) {
    g_logger->SetStream(nullptr);
    g_logger->SetMinLevel(Logger::FATAL);
  } else {
    g_log_stream->open(std::string(g_log_file), std::ios::app);
    if (!g_log_stream->good()) {
      return Status(Status::SYSTEM_ERROR, "log open failed");
    }
    g_logger->SetStream(g_log_stream);
    if (g_log_level == "debug") {
      g_logger->SetMinLevel(Logger::DEBUG);
    } else if (g_log_level == "info") {
      g_logger->SetMinLevel(Logger::INFO);
    } else if (g_log_level == "warn") {
      g_logger->SetMinLevel(Logger::WARN);
    } else if (g_log_level == "error") {
      g_logger->SetMinLevel(Logger::ERROR);
    } else if (g_log_level == "fatal") {
      g_logger->SetMinLevel(Logger::FATAL);
    }
  }
  return Status(Status::SUCCESS);
}

// Reconfigure the server.
void ReconfigServer(int signum) {
  grpc::Server* server = g_server.load();
  if (server != nullptr) {
    g_logger->Log(Logger::INFO, StrCat("Reconfiguring by signal: ", signum));
    const Status status = ConfigLogger();
    if (status != Status::SUCCESS) {
      g_logger->Log(Logger::ERROR, StrCat("ConfigLogger failed: ", status));
    }
  }
}

// Shutdowns the server.
void ShutdownServer(int signum) {
  grpc::Server* server = g_server.load();
  if (server != nullptr && g_server.compare_exchange_strong(server, nullptr)) {
    g_logger->Log(Logger::INFO, StrCat("Shutting down by signal: ", signum));
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
    server->Shutdown(deadline);
  }
}

// Processes the command.
static int32_t Process(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--version", 0}, {"--host", 1}, {"--port", 1},
    {"--log_file", 1}, {"--log_level", 1}, {"--pid_file", 1},
    {"--daemon", 0},
    {"--read_only", 0},
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
  const std::string log_file = GetStringArgument(cmd_args, "--log_file", 0, "/dev/stdout");
  const std::string log_level = GetStringArgument(cmd_args, "--log_level", 0, "info");
  const std::string pid_file = GetStringArgument(cmd_args, "--pid_file", 0, "");
  const bool as_daemon = CheckMap(cmd_args, "--daemon");
  const bool read_only = CheckMap(cmd_args, "--read_only");
  auto dbm_exprs = SearchMap(cmd_args, "", {});
  if (dbm_exprs.empty()) {
    dbm_exprs.emplace_back("#dbm=tiny");
  }
  if (as_daemon) {
    const Status status = DaemonizeProcess();
    if (status != Status::SUCCESS) {
      EPrintL("DaemonizeProcess failed: ", status);
      return 1;
    }
  }
  StreamLogger logger;
  g_logger = &logger;
  g_log_file = std::string_view(log_file);
  g_log_level = std::string_view(log_level);
  std::ofstream log_stream;
  g_log_stream = &log_stream;
  ConfigLogger();
  bool has_error = false;
  const int32_t pid = GetProcessID();
  logger.LogF(Logger::INFO, "==== Starting the process %s ====",
              (as_daemon ? "as a daemon" : "as a command"));
  if (!pid_file.empty()) {
    logger.Log(Logger::INFO, StrCat("Writing the PID file: ", pid_file));
    const Status status = WriteFile(pid_file, StrCat(pid, "\n"));
    if (status != Status::SUCCESS) {
      logger.Log(Logger::ERROR, StrCat("WriteFile failed: ", pid_file, ": ", status));
      has_error = true;
    }
  }
  std::vector<std::unique_ptr<ParamDBM>> dbms;
  dbms.reserve(dbm_exprs.size());
  for (const auto& dbm_expr : dbm_exprs) {
    logger.Log(Logger::INFO, StrCat("Opening a database: ", dbm_expr));
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
    const bool writable = read_only ? false : true;
    const Status status = dbm->OpenAdvanced(path, writable, File::OPEN_DEFAULT, params);
    if (status != Status::SUCCESS) {
      logger.Log(Logger::ERROR, StrCat("Open failed: ", path, ": ", status));
      has_error = true;
    }
    dbms.emplace_back(std::move(dbm));
  }
  const std::string server_address(StrCat(host, ":", port));
  DBMServiceImpl service(dbms, &logger);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  logger.Log(Logger::INFO, StrCat("address=", server_address, ", pid=", pid));
  g_server.store(server.get());
  std::signal(SIGHUP, ReconfigServer);
  std::signal(SIGINT, ShutdownServer);
  std::signal(SIGTERM, ShutdownServer);
  std::signal(SIGQUIT, ShutdownServer);
  server->Wait();
  logger.Log(Logger::INFO, "The server finished");
  for (auto& dbm : dbms) {
    logger.Log(Logger::INFO, "Closing a database");
    const Status status = dbm->Close();
    if (status != Status::SUCCESS) {
      logger.Log(Logger::ERROR, StrCat("Close failed: ", status));
      has_error = true;
    }
  }
  logger.LogF(Logger::INFO, "==== Ending the process %s ====",
              (has_error ? "with errors" : "in success"));
  return has_error ? 1 : 0;
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
