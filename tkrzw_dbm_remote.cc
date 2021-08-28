/*************************************************************************************************
 * Remote database manager implementation based on gRPC
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

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "tkrzw_dbm_remote.h"
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

class RemoteDBMImpl final {
 public:
  RemoteDBMImpl();
  void InfectStub(void* stub);
  Status Connect(const std::string& host, int32_t port);
  void Disconnect();
  void SetDBMIndex(int32_t dbm_index);
  Status Echo(std::string_view message, std::string* echo);
  Status Inspect(std::vector<std::pair<std::string, std::string>>* records);
  Status Get(std::string_view key, std::string* value);
  Status Set(std::string_view key, std::string_view value, bool overwrite);
  Status Remove(std::string_view key);
  Status Append(std::string_view key, std::string_view value, std::string_view delim);
  Status Increment(std::string_view key, int64_t increment, int64_t* current, int64_t initial);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* file_size);
  Status Clear();
  Status Rebuild(const std::map<std::string, std::string>& params);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, const std::map<std::string, std::string>& params);

 private:
  std::unique_ptr<DBMService::StubInterface> stub_;
  int32_t dbm_index_;
};

RemoteDBMImpl::RemoteDBMImpl() : stub_(nullptr), dbm_index_(0) {}

void RemoteDBMImpl::InfectStub(void* stub) {
  stub_.reset(reinterpret_cast<DBMService::StubInterface*>(stub));
}

Status RemoteDBMImpl::Connect(const std::string& host, int32_t port) {
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

void RemoteDBMImpl::Disconnect() {
  stub_.reset(nullptr);
}

void RemoteDBMImpl::SetDBMIndex(int32_t dbm_index) {
  dbm_index_ = dbm_index;
}

Status RemoteDBMImpl::Echo(std::string_view message, std::string* echo) {
  grpc::ClientContext context;
  EchoRequest request;
  request.set_message(std::string(message));
  EchoResponse response;
  grpc::Status status = stub_->Echo(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  *echo = response.echo();
  return Status(Status::SUCCESS);
}

Status RemoteDBMImpl::Inspect(std::vector<std::pair<std::string, std::string>>* records) {
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

Status RemoteDBMImpl::Get(std::string_view key, std::string* value) {
  grpc::ClientContext context;
  GetRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  GetResponse response;
  grpc::Status status = stub_->Get(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (response.status().code() == 0 && value != nullptr) {
    *value = response.value();
  }
  return MakeStatusFromProto(response.status());
}

Status RemoteDBMImpl::Set(std::string_view key, std::string_view value, bool overwrite) {
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

Status RemoteDBMImpl::Remove(std::string_view key) {
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

Status RemoteDBMImpl::Append(
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

Status RemoteDBMImpl::Increment(
    std::string_view key, int64_t increment, int64_t* current, int64_t initial) {
  grpc::ClientContext context;
  IncrementRequest request;
  request.set_dbm_index(dbm_index_);
  request.set_key(key.data(), key.size());
  request.set_increment(increment);
  request.set_initial(initial);
  IncrementResponse response;
  grpc::Status status = stub_->Increment(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (current != nullptr) {
    *current = response.current();
  }
  return MakeStatusFromProto(response.status());
}

Status RemoteDBMImpl::Count(int64_t* count) {
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

Status RemoteDBMImpl::GetFileSize(int64_t* file_size) {
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

Status RemoteDBMImpl::Clear() {
  grpc::ClientContext context;
  ClearRequest request;
  ClearResponse response;
  grpc::Status status = stub_->Clear(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

Status RemoteDBMImpl::Rebuild(const std::map<std::string, std::string>& params) {
  grpc::ClientContext context;
  RebuildRequest request;
  for (const auto& param : params) {
    auto* req_param = request.add_params();
    req_param->set_first(param.first);
    req_param->set_second(param.second);
  }
  RebuildResponse response;
  grpc::Status status = stub_->Rebuild(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

Status RemoteDBMImpl::ShouldBeRebuilt(bool* tobe) {
  grpc::ClientContext context;
  ShouldBeRebuiltRequest request;
  ShouldBeRebuiltResponse response;
  grpc::Status status = stub_->ShouldBeRebuilt(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  if (response.status().code() == 0) {
    *tobe = response.tobe();
  }
  return MakeStatusFromProto(response.status());
}

Status RemoteDBMImpl::Synchronize(bool hard, const std::map<std::string, std::string>& params) {
  grpc::ClientContext context;
  SynchronizeRequest request;
  request.set_hard(hard);
  for (const auto& param : params) {
    auto* req_param = request.add_params();
    req_param->set_first(param.first);
    req_param->set_second(param.second);
  }
  SynchronizeResponse response;
  grpc::Status status = stub_->Synchronize(&context, request, &response);
  if (!status.ok()) {
    return Status(Status::NETWORK_ERROR, GRPCStatusString(status));
  }
  return MakeStatusFromProto(response.status());
}

RemoteDBM::RemoteDBM() : impl_(nullptr) {
  impl_ = new RemoteDBMImpl();
}

RemoteDBM::~RemoteDBM() {
  delete impl_;
}

void RemoteDBM::InjectStub(void* stub) {
  impl_->InfectStub(stub);
}

Status RemoteDBM::Connect(const std::string& host, int32_t port) {
  return impl_->Connect(host, port);
}

void RemoteDBM::Disconnect() {
  return impl_->Disconnect();
}

void RemoteDBM::SetDBMIndex(int32_t dbm_index) {
  return impl_->SetDBMIndex(dbm_index);
}

Status RemoteDBM::Echo(std::string_view message, std::string* echo) {
  return impl_->Echo(message, echo);
}

Status RemoteDBM::Inspect(std::vector<std::pair<std::string, std::string>>* records) {
  return impl_->Inspect(records);
}

Status RemoteDBM::Get(std::string_view key, std::string* value) {
  return impl_->Get(key, value);
}

Status RemoteDBM::Set(std::string_view key, std::string_view value, bool overwrite) {
  return impl_->Set(key, value, overwrite);
}

Status RemoteDBM::Remove(std::string_view key) {
  return impl_->Remove(key);
}

Status RemoteDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  return impl_->Append(key, value, delim);
}

Status RemoteDBM::Increment(
    std::string_view key, int64_t increment, int64_t* current, int64_t initial) {
  return impl_->Increment(key, increment, current, initial);
}

Status RemoteDBM::Count(int64_t* count) {
  return impl_->Count(count);
}

Status RemoteDBM::GetFileSize(int64_t* file_size) {
  return impl_->GetFileSize(file_size);
}

Status RemoteDBM::Clear() {
  return impl_->Clear();
}

Status RemoteDBM::Rebuild(const std::map<std::string, std::string>& params) {
  return impl_->Rebuild(params);
}

Status RemoteDBM::ShouldBeRebuilt(bool* tobe) {
  return impl_->ShouldBeRebuilt(tobe);
}

Status RemoteDBM::Synchronize(bool hard, const std::map<std::string, std::string>& params) {
  return impl_->Synchronize(hard, params);
}

}  // namespace tkrzw

// END OF FILE
