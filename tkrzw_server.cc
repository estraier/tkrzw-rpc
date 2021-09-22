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
#include "tkrzw_rpc_common.h"
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
  P("  --version : Prints the version number and exits.\n");
  P("  --address str : The address/hostname and the port of the server (default: 0.0.0.0:1978)\n");
  P("  --async : Uses the asynchronous API on ths server.\n");
  P("  --threads num : The maximum number of worker threads. (default: 1)\n");
  P("  --log_file str : The file path of the log file. (default: /dev/stdout)\n");
  P("  --log_level str : The minimum log level to be stored:"
    " debug, info, warn, error, fatal. (default: info)\n");
  P("  --log_date str : The log date format: simple, simple_micro, w3cdtf, w3cdtf_micro,"
    " rfc1123, epoch, epoch_micro. (default: simple)\n");
  P("  --log_td num : The log time difference in seconds. (default: 99999=local)\n");
  P("  --server_id num: The server ID. (default: 1)\n");
  P("  --ulog_prefix str : The prefix of the update log files.\n");
  P("  --ulog_max_file_size num : The maximum file size of each update log file."
    " (default: 1Gi)\n");
  P("  --pid_file str : The file path of the store the process ID.\n");
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
std::string_view g_log_date;
int32_t g_log_td = 0;
std::ofstream* g_log_stream = nullptr;
MessageQueue* g_mq = nullptr;
std::atomic<grpc::Server*> g_server(nullptr);
bool g_is_shutdown = false;

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
    g_logger->SetMinLevel(Logger::LEVEL_NONE);
  } else {
    g_log_stream->open(std::string(g_log_file), std::ios::app);
    if (!g_log_stream->good()) {
      return Status(Status::SYSTEM_ERROR, "log open failed");
    }  std::unique_ptr<MessageQueue> mq;
    g_logger->SetStream(g_log_stream);
    g_logger->SetMinLevel(Logger::ParseLevelStr(g_log_level));
    g_logger->SetDateFormat(BaseLogger::ParseDateFormatStr(g_log_date), g_log_td);
  }
  return Status(Status::SUCCESS);
}

// Reconfigure the server.
void ReconfigServer(int signum) {
  grpc::Server* server = g_server.load();
  if (server != nullptr) {
    g_logger->LogCat(Logger::LEVEL_INFO, "Reconfiguring by signal: ", signum);
    const Status status = ConfigLogger();
    if (status != Status::SUCCESS) {
      g_logger->LogCat(Logger::LEVEL_ERROR, "ConfigLogger failed: ", status);
    }
  }
}

// Shutdowns the server.
void ShutdownServer(int signum) {
  grpc::Server* server = g_server.load();
  if (server != nullptr && g_server.compare_exchange_strong(server, nullptr)) {
    g_logger->LogCat(Logger::LEVEL_INFO, "Shutting down by signal: ", signum);
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
    g_is_shutdown = true;
    if (g_mq != nullptr) {
      g_mq->CancelReaders();
    }
    server->Shutdown(deadline);
  }
}

// Processes the command.
static int32_t Process(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--version", 0}, {"--address", 1}, {"--async", 0}, {"--threads", 1},
    {"--log_file", 1}, {"--log_level", 1}, {"--log_date", 1}, {"--log_td", 1},
    {"--server_id", 1}, {"--ulog_prefix", 1}, {"--ulog_max_file_size", 1},
    {"--pid_file", 1}, {"--daemon", 0},
    {"--read_only", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  if (CheckMap(cmd_args, "--version")) {
    PrintL("Tkrzw-RPC server ", RPC_PACKAGE_VERSION);
    return 0;
  }
  const std::string address = GetStringArgument(cmd_args, "--address", 0, "0.0.0.0:1978");
  const bool with_async = CheckMap(cmd_args, "--async");
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const std::string log_file = GetStringArgument(cmd_args, "--log_file", 0, "/dev/stdout");
  const std::string log_level = GetStringArgument(cmd_args, "--log_level", 0, "info");
  const std::string log_date = GetStringArgument(cmd_args, "--log_date", 0, "simple");
  const int32_t log_td = GetIntegerArgument(cmd_args, "--log_td", 0, 99999);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, 1);
  const std::string ulog_prefix = GetStringArgument(cmd_args, "--ulog_prefix", 0, "");
  const int64_t ulog_max_file_size =
      GetIntegerArgument(cmd_args, "--ulog_max_file_size", 0, 1LL << 30);
  const std::string pid_file = GetStringArgument(cmd_args, "--pid_file", 0, "");
  const bool as_daemon = CheckMap(cmd_args, "--daemon");
  const bool read_only = CheckMap(cmd_args, "--read_only");
  auto dbm_exprs = SearchMap(cmd_args, "", {});
  if (address.find(":") == std::string::npos) {
    Die("Invalid address");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  if (server_id < 1) {
    Die("Invalid server ID");
  }
  if (dbm_exprs.empty()) {
    dbm_exprs.emplace_back("#dbm=tiny");
  }
  if (dbm_exprs.size() > 200) {
    Die("Too many databases");
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
  g_log_date = std::string_view(log_date);
  g_log_td = log_td < 99999 ? log_td : INT32MIN;
  std::ofstream log_stream;
  g_log_stream = &log_stream;
  ConfigLogger();
  SetGlobalLogger(&logger);
  bool has_error = false;
  const int32_t pid = GetProcessID();
  logger.LogF(Logger::LEVEL_INFO, "======== Starting the process %d %s ========",  pid,
              (as_daemon ? "as a daemon" : "as a command"));
  logger.LogCat(Logger::LEVEL_INFO, "Version: ", "rpc_pkg=", RPC_PACKAGE_VERSION,
                ", rpc_lib=", RPC_LIBRARY_VERSION,
                ", core_pkg=", PACKAGE_VERSION,
                ", core_lib=", LIBRARY_VERSION);
  if (!pid_file.empty()) {
    logger.LogCat(Logger::LEVEL_INFO, "Writing the PID file: ", pid_file);
    const Status status = WriteFile(pid_file, StrCat(pid, "\n"));
    if (status != Status::SUCCESS) {
      logger.LogCat(Logger::LEVEL_ERROR, "WriteFile failed: ", pid_file, ": ", status);
      has_error = true;
    }
  }
  std::unique_ptr<MessageQueue> mq;
  if (!ulog_prefix.empty()) {
    mq = std::make_unique<MessageQueue>();
    logger.LogCat(Logger::LEVEL_INFO, "Opening the message queue: ", ulog_prefix);
    const Status status = mq->Open(ulog_prefix, ulog_max_file_size);
    if (status != Status::SUCCESS) {
      logger.LogCat(Logger::LEVEL_ERROR, "Open failed: ", ulog_prefix);
      has_error = true;
    }
    g_mq = mq.get();
  }
  std::vector<std::unique_ptr<ParamDBM>> dbms;
  dbms.reserve(dbm_exprs.size());
  std::vector<std::unique_ptr<DBMUpdateLoggerMQ>> ulogs;
  ulogs.reserve(dbm_exprs.size());
  for (const auto& dbm_expr : dbm_exprs) {
    logger.LogCat(Logger::LEVEL_INFO, "Opening a database: ", dbm_expr);
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
      logger.LogCat(Logger::LEVEL_ERROR, "Open failed: ", path, ": ", status);
      has_error = true;
    }
    if (mq != nullptr) {
      auto ulog = std::make_unique<DBMUpdateLoggerMQ>(mq.get(), server_id, dbms.size());
      dbm->SetUpdateLogger(ulog.get());
      ulogs.emplace_back(std::move(ulog));
    }
    dbms.emplace_back(std::move(dbm));
  }
  logger.LogCat(Logger::LEVEL_INFO,
                "Building the ", (with_async > 0 ? "async" : "sync"),
                " server: address=", address, ", id=", server_id);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  std::unique_ptr<grpc::Service> service;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> async_queues;
  if (with_async) {
    service = std::make_unique<DBMAsyncServiceImpl>(dbms, &logger, mq.get());
    builder.RegisterService(service.get());
    async_queues.resize(num_threads);
    for (auto& async_queue : async_queues) {
      async_queue = builder.AddCompletionQueue();
    }
  } else {
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, num_threads);
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::CQ_TIMEOUT_MSEC, 60000);
    service = std::make_unique<DBMServiceImpl>(dbms, &logger, mq.get());
    builder.RegisterService(service.get());
  }
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (server == nullptr) {
    logger.LogCat(Logger::LEVEL_FATAL, "ServerBuilder::BuildAndStart failed: ", address);
    has_error = true;
  } else {
    g_server.store(server.get());
    std::signal(SIGHUP, ReconfigServer);
    std::signal(SIGINT, ShutdownServer);
    std::signal(SIGTERM, ShutdownServer);
    std::signal(SIGQUIT, ShutdownServer);
    if (with_async > 0) {
      auto* async_service = (DBMAsyncServiceImpl*)service.get();
      auto task =
          [&](grpc::ServerCompletionQueue* queue) {
            async_service->OperateQueue(queue, &g_is_shutdown);
          };
      std::vector<std::thread> threads;
      for (auto& queue : async_queues) {
        threads.emplace_back(std::thread(task, queue.get()));
      }
      for (auto& thread : threads) {
        thread.join();
      }
      for (auto& queue : async_queues) {
        async_service->ShutdownQueue(queue.get());
      }
    } else {
      server->Wait();
    }
    logger.Log(Logger::LEVEL_INFO, "The server finished");
  }
  for (auto& dbm : dbms) {
    logger.Log(Logger::LEVEL_INFO, "Closing a database");
    const Status status = dbm->Close();
    if (status != Status::SUCCESS) {
      logger.LogCat(Logger::LEVEL_ERROR, "Close failed: ", status);
      has_error = true;
    }
  }
  if (mq != nullptr) {
    logger.Log(Logger::LEVEL_INFO, "Closing the message queue");
    const Status status = mq->Close();
    if (status != Status::SUCCESS) {
      logger.LogCat(Logger::LEVEL_ERROR, "Close failed: ", status);
      has_error = true;
    }
  }
  logger.LogF(Logger::LEVEL_INFO, "======== Ending the process %d %s ========",
              pid, (has_error ? "with errors" : "in success"));
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
