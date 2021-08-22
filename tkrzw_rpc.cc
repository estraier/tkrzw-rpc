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

class DBMClientImpl final {
 public:
  void InfectStub(void* stub);
  Status Connect(const std::string& host, int32_t port);
  void Disconnect();
  Status GetVersion(std::string* version);

 private:
  std::unique_ptr<DBMService::StubInterface> stub_;
};

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

Status DBMClient::GetVersion(std::string* version) {
  return impl_->GetVersion(version);
}

}  // namespace tkrzw

// END OF FILE
