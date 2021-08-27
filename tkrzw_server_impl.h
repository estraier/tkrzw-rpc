/*************************************************************************************************
 * RPC server implementation of Tkrzw
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

#ifndef _TKRZW_SERVER_IMPL_H
#define _TKRZW_SERVER_IMPL_H

#include <cassert>
#include <cstdarg>
#include <cstdint>

#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include <google/protobuf/message.h>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "tkrzw_cmd_util.h"
#include "tkrzw_rpc.grpc.pb.h"
#include "tkrzw_rpc.h"
#include "tkrzw_rpc.pb.h"

namespace tkrzw {

class DBMServiceImpl : public DBMService::Service {
 public:
  DBMServiceImpl(const std::vector<std::unique_ptr<ParamDBM>>& dbms, Logger* logger)
      : dbms_(dbms), logger_(logger) {}

  void LogRequest(grpc::ServerContext* context, const char* name,
                  const google::protobuf::Message* proto) {
    if (!logger_->CheckLevel(Logger::DEBUG)) {
      return;
    }
    static std::regex regex_linehead("\\n\\s*");
    std::string proto_text =  std::regex_replace(proto->Utf8DebugString(), regex_linehead, " ");
    while (!proto_text.empty() && proto_text.back() == ' ') {
      proto_text.resize(proto_text.size() - 1);
    }
    std::string message = StrCat(context->peer(), " [", name, "]");
    if (!proto_text.empty()) {
      message += " ";
      message += proto_text;
    }
    logger_->Log(Logger::DEBUG, message);
  }

  grpc::Status GetVersion(
      grpc::ServerContext* context, const GetVersionRequest* request,
      GetVersionResponse* response) override {
    LogRequest(context, "GetVersion", request);
    response->set_version(_TKSERV_PKG_VERSION);
    return grpc::Status::OK;
  }

  grpc::Status Inspect(
      grpc::ServerContext* context, const InspectRequest* request,
      InspectResponse* response) override {
    LogRequest(context, "Inspect", request);
    if (request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    if (request->dbm_index() >= 0) {
      auto& dbm = *dbms_[request->dbm_index()];
      for (const auto& record : dbm.Inspect()) {
        auto* out_rec = response->add_records();
        out_rec->set_first(record.first);
        out_rec->set_second(record.second);
      }
    } else {
      for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
        auto& dbm = *dbms_[i];
        auto* out_record = response->add_records();
        out_record->set_first(StrCat("dbm_", i, "_path"));
        out_record->set_second(ToString(dbm.GetFilePathSimple()));
        out_record = response->add_records();
        out_record->set_first(StrCat("dbm_", i, "_count"));
        out_record->set_second(ToString(dbm.CountSimple()));
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status Get(
      grpc::ServerContext* context, const GetRequest* request,
      GetResponse* response) override {
    LogRequest(context, "Get", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::string value;
    const Status status = dbm.Get(request->key(), &value);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_value(value);
    }
    return grpc::Status::OK;
  }

  grpc::Status Set(
      grpc::ServerContext* context, const SetRequest* request,
      SetResponse* response) override {
    LogRequest(context, "Set", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.Set(request->key(), request->value(), request->overwrite());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status Remove(
      grpc::ServerContext* context, const RemoveRequest* request,
      RemoveResponse* response) override {
    LogRequest(context, "Remove", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.Remove(request->key());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status Append(
      grpc::ServerContext* context, const AppendRequest* request,
      AppendResponse* response) override {
    LogRequest(context, "Append", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.Append(request->key(), request->value(), request->delim());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status Increment(
      grpc::ServerContext* context, const IncrementRequest* request,
      IncrementResponse* response) override {
    LogRequest(context, "Increment", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    int64_t current = 0;
    const Status status =
        dbm.Increment(request->key(), request->increment(), &current, request->initial());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_current(current);
    }
    return grpc::Status::OK;
  }

  grpc::Status Count(
      grpc::ServerContext* context, const CountRequest* request,
      CountResponse* response) override {
    LogRequest(context, "Count", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    int64_t count = 0;
    const Status status = dbm.Count(&count);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_count(count);
    }
    return grpc::Status::OK;
  }

  grpc::Status GetFileSize(
      grpc::ServerContext* context, const GetFileSizeRequest* request,
      GetFileSizeResponse* response) override {
    LogRequest(context, "GetFileSize", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    int64_t file_size = 0;
    const Status status = dbm.GetFileSize(&file_size);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_file_size(file_size);
    }
    return grpc::Status::OK;
  }

  grpc::Status Clear(
      grpc::ServerContext* context, const ClearRequest* request,
      ClearResponse* response) override {
    LogRequest(context, "Clear", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.Clear();
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status Rebuild(
      grpc::ServerContext* context, const RebuildRequest* request,
      RebuildResponse* response) override {
    LogRequest(context, "Rebuild", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::map<std::string, std::string> params;
    for (const auto& param : request->params()) {
      params.emplace(std::make_pair(param.first(), param.second()));
    }
    const Status status = dbm.RebuildAdvanced(params);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status ShouldBeRebuilt(
      grpc::ServerContext* context, const ShouldBeRebuiltRequest* request,
      ShouldBeRebuiltResponse* response) override {
    LogRequest(context, "ShouldBeRebuilt", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    bool tobe = false;
    const Status status = dbm.ShouldBeRebuilt(&tobe);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_tobe(tobe);
    }
    return grpc::Status::OK;
  }

  grpc::Status Synchronize(
      grpc::ServerContext* context, const SynchronizeRequest* request,
      SynchronizeResponse* response) override {
    LogRequest(context, "Synchronize", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::map<std::string, std::string> params;
    for (const auto& param : request->params()) {
      params.emplace(std::make_pair(param.first(), param.second()));
    }
    const Status status = dbm.SynchronizeAdvanced(request->hard(), nullptr, params);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

 private:
  const std::vector<std::unique_ptr<ParamDBM>>& dbms_;
  Logger* logger_;
};

}  // namespace tkrzw

#endif  // _TKRZW_SERVER_IMPL_H

// END OF FILE
