/*************************************************************************************************
 * Common library features
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

#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <fcntl.h>

#include <grpc/impl/codegen/log.h>
#include <grpc/impl/codegen/port_platform.h>

#include "tkrzw_cmd_util.h"
#include "tkrzw_rpc_common.h"

namespace tkrzw {

const char* const RPC_PACKAGE_VERSION = _TKRPC_PKG_VERSION;;
const char* const RPC_LIBRARY_VERSION = _TKRPC_LIB_VERSION;;

static tkrzw::Logger* g_logger = nullptr;

static void PrintGlobalLog(gpr_log_func_args *args) {
  if (g_logger == nullptr) {
    return;
  }
  Logger::Level log_level = Logger::LEVEL_DEBUG;
  if (args->severity == GPR_LOG_SEVERITY_INFO) {
    log_level = Logger::LEVEL_INFO;
  } else if (args->severity == GPR_LOG_SEVERITY_ERROR) {
    log_level = Logger::LEVEL_ERROR;
  }
  g_logger->LogF(log_level, "GPR: %s: %d: %s", args->file, args->line, args->message);
}

void SetGlobalLogger(tkrzw::Logger* logger) {
  g_logger = logger;
  gpr_set_log_function(PrintGlobalLog);
}

Status DaemonizeProcess() {
  std::cout << std::flush;
  std::cerr << std::flush;
  switch (fork()) {
    case -1: {
      return GetErrnoStatus("fork", errno);
    }
    case 0: {
      break;
    }
    default: {
      _exit(0);
    }
  }
  if (setsid() == -1) {
    return GetErrnoStatus("setsid", errno);
  }
  signal(SIGHUP, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);
  switch (fork()) {
    case -1: {
      return GetErrnoStatus("fork", errno);
    }
    case 0: {
      break;
    }
    default: {
      _exit(0);
    }
  }
  umask(0);
  if (chdir("/") == -1) {
    return GetErrnoStatus("chdir", errno);
  }
  close(0);
  close(1);
  close(2);
  const int32_t fd = open("/dev/null", O_RDWR, 0);
  if (fd != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

// END OF FILE
