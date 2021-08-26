/*************************************************************************************************
 * RPC API of Tkrzw
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

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "tkrzw_rpc.h"
#include "tkrzw_rpc.grpc.pb.h"
#include "tkrzw_rpc.pb.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

const char* GRPCStatusCodeName(grpc::StatusCode code) {
  switch (code) {
    case grpc::StatusCode::OK: return "OK";
    case grpc::StatusCode::CANCELLED: return "CANCELLED";
    case grpc::StatusCode::UNKNOWN: return "UNKNOWN";
    case grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
    case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
    case grpc::StatusCode::NOT_FOUND: return "NOT_FOUND";
    case grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
    case grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
    case grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED";
    case grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
    case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
    case grpc::StatusCode::ABORTED: return "ABORTED";
    case grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE";
    case grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED";
    case grpc::StatusCode::INTERNAL: return "INTERNAL";
    case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
    case grpc::StatusCode::DATA_LOSS: return "DATA_LOSS";
    default: break;
  }
  return "unknown";
}

std::string GRPCStatusString(const grpc::Status& status) {
  const std::string& msg = status.error_message();
  if (msg.empty()) {
    return GRPCStatusCodeName(status.error_code());
  }
  return StrCat(GRPCStatusCodeName(status.error_code()), ": ", msg);
}

Status MakeStatusFromProto(const tkrzw::StatusProto& proto) {
  const auto& message = proto.message();
  if (message.empty()) {
    return Status(tkrzw::Status::Code(proto.code()));
  }
  return Status(tkrzw::Status::Code(proto.code()), message);
}

class DBMClientImpl final {
 public:
  DBMClientImpl();
  void InfectStub(void* stub);
  Status Connect(const std::string& host, int32_t port);
  void Disconnect();
  void SetDBMIndex(int32_t dbm_index);
  Status GetVersion(std::string* version);
  Status Inspect(std::vector<std::pair<std::string, std::string>>* records);
  Status Get(std::string_view key, std::string* value);
  Status Set(std::string_view key, std::string_view value, bool overwrite);
  Status Remove(std::string_view key);
  Status Append(std::string_view key, std::string_view value, std::string_view delim);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* file_size);

 private:
  std::unique_ptr<DBMService::StubInterface> stub_;
  int32_t dbm_index_;
};

DBMClientImpl::DBMClientImpl() : stub_(nullptr), dbm_index_(0) {}

void DBMClientImpl::InfectStub(void* stub) {
  stub_.reset(reinterpret_cast<DBMService::StubInterface*>(stub));
}

Status DBMClientImpl::Connect(const std::string& host, int32_t port) {
  const std::string server_address(StrCat(host, ":", port));
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  while (true) {
    auto status = channel->GetState(true);
    if (status == GRPC_CHANNEL_READY) {
      break;
    }
    if ((status != GRPC_CHANNEL_IDLE && status != GRPC_CHANNEL_CONNECTING) ||
        !channel->WaitForStateChange(status, deadline)) {
      return Status(Status::NETWORK_ERROR, "connection failed");
    }
  }
  stub_ = DBMService::NewStub(channel);
  return Status(Status::SUCCESS);
}

void DBMClientImpl::Disconnect() {
  stub_.reset(nullptr);
}

void DBMClientImpl::SetDBMIndex(int32_t dbm_index) {
  dbm_index_ = dbm_index;
}

Status DBMClientImpl::GetVersion(std::string* version) {
  grpc::ClientContext context;
  GetVersionRequest request;
  GetVersionResponse response;
  grpc::Status status = stub_->GetVersion(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  *version = response.version();
  return Status(Status::SUCCESS);
}

Status DBMClientImpl::Inspect(std::vector<std::pair<std::string, std::string>>* records) {
  grpc::ClientContext context;
  InspectRequest request;
  request.set_dbm_index(dbm_index_);
  InspectResponse response;
  grpc::Status status = stub_->Inspect(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  for (const auto& record : response.records()) {
    records->emplace_back(std::make_pair(record.first(), record.second()));
  }
  return Status(Status::SUCCESS);
}

Status DBMClientImpl::Get(std::string_view key, std::string* value) {
  grpc::ClientContext context;
  GetRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  GetResponse response;
  grpc::Status status = stub_->Get(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (response.status().code() == 0) {
    *value = response.value();
  }
  return MakeStatusFromProto(response.status());
}

Status DBMClientImpl::Set(std::string_view key, std::string_view value, bool overwrite) {
  grpc::ClientContext context;
  SetRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  request.set_value(value.data(), value.size());
  request.set_overwrite(overwrite);
  SetResponse response;
  grpc::Status status = stub_->Set(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

Status DBMClientImpl::Remove(std::string_view key) {
  grpc::ClientContext context;
  RemoveRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  RemoveResponse response;
  grpc::Status status = stub_->Remove(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

Status DBMClientImpl::Append(
    std::string_view key, std::string_view value, std::string_view delim) {
  grpc::ClientContext context;
  AppendRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  request.set_value(value.data(), value.size());
  request.set_delim(delim.data(), delim.size());
  AppendResponse response;
  grpc::Status status = stub_->Append(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

Status DBMClientImpl::Count(int64_t* count) {
  grpc::ClientContext context;
  CountRequest request;
  CountResponse response;
  grpc::Status status = stub_->Count(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (response.status().code() == 0) {
    *count = response.count();
  }
  return MakeStatusFromProto(response.status());
}

Status DBMClientImpl::GetFileSize(int64_t* file_size) {
  grpc::ClientContext context;
  GetFileSizeRequest request;
  GetFileSizeResponse response;
  grpc::Status status = stub_->GetFileSize(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (response.status().code() == 0) {
    *file_size = response.file_size();
  }
  return MakeStatusFromProto(response.status());
}

DBMClient::DBMClient() : impl_(nullptr) {
  impl_ = new DBMClientImpl();
}

DBMClient::~DBMClient() {
  delete impl_;
}

void DBMClient::InjectStub(void* stub) {
  impl_->InfectStub(stub);
}

Status DBMClient::Connect(const std::string& host, int32_t port) {
  return impl_->Connect(host, port);
}

void DBMClient::Disconnect() {
  return impl_->Disconnect();
}

void DBMClient::SetDBMIndex(int32_t dbm_index) {
  return impl_->SetDBMIndex(dbm_index);
}

Status DBMClient::GetVersion(std::string* version) {
  return impl_->GetVersion(version);
}

Status DBMClient::Inspect(std::vector<std::pair<std::string, std::string>>* records) {
  return impl_->Inspect(records);
}

Status DBMClient::Get(std::string_view key, std::string* value) {
  return impl_->Get(key, value);
}

Status DBMClient::Set(std::string_view key, std::string_view value, bool overwrite) {
  return impl_->Set(key, value, overwrite);
}

Status DBMClient::Remove(std::string_view key) {
  return impl_->Remove(key);
}

Status DBMClient::Append(std::string_view key, std::string_view value, std::string_view delim) {
  return impl_->Append(key, value, delim);
}

Status DBMClient::Count(int64_t* count) {
  return impl_->Count(count);
}

Status DBMClient::GetFileSize(int64_t* file_size) {
  return impl_->GetFileSize(file_size);
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
