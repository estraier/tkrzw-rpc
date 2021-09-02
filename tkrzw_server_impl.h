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
#include "tkrzw_rpc_common.h"
#include "tkrzw_rpc.grpc.pb.h"
#include "tkrzw_rpc.pb.h"

namespace tkrzw {

class DBMServiceImpl : public DBMService::Service {
 public:
  DBMServiceImpl(const std::vector<std::unique_ptr<ParamDBM>>& dbms, Logger* logger)
      : dbms_(dbms), logger_(logger) {}

  void LogRequest(grpc::ServerContext* context, const char* name,
                  const google::protobuf::Message* proto) {
    if (!logger_->CheckLevel(Logger::LEVEL_DEBUG)) {
      return;
    }
    static std::regex regex_linehead("\\n\\s*");
    std::string proto_text =  std::regex_replace(proto->Utf8DebugString(), regex_linehead, " ");
    while (!proto_text.empty() && proto_text.back() == ' ') {
      proto_text.resize(proto_text.size() - 1);
    }
    std::string message;
    const std::string peer = context->peer();
    if (StrBeginsWith(peer, "ipv4:")) {
      message += peer.substr(5) + " ";
    } else if (StrBeginsWith(peer, "ipv6:")) {
      message += peer.substr(5) + " ";
    }
    message += "[";
    message += name;
    message += "]";
    if (!proto_text.empty()) {
      message += " ";
      message += proto_text;
    }
    logger_->Log(Logger::LEVEL_DEBUG, message);
  }

  grpc::Status Echo(
      grpc::ServerContext* context, const EchoRequest* request,
      EchoResponse* response) override {
    LogRequest(context, "Echo", request);
    response->set_echo(request->message());
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
      auto* out_record = response->add_records();
      out_record->set_first("version");
      out_record->set_second(RPC_PACKAGE_VERSION);
      out_record = response->add_records();
      out_record->set_first("num_dbms");
      out_record->set_second(ToString(dbms_.size()));
      for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
        auto& dbm = *dbms_[i];
        out_record = response->add_records();
        out_record->set_first(StrCat("dbm_", i, "_path"));
        out_record->set_second(ToString(dbm.GetFilePathSimple()));
        out_record = response->add_records();
        out_record->set_first(StrCat("dbm_", i, "_count"));
        out_record->set_second(ToString(dbm.CountSimple()));
        std::string class_name;
        for (const auto& record : dbm.Inspect()) {
          if (record.first == "class") {
            class_name = record.second;
          }
        }
        out_record = response->add_records();
        out_record->set_first(StrCat("dbm_", i, "_class"));
        out_record->set_second(class_name);
      }
      out_record = response->add_records();
      out_record->set_first("memory_usage");
      out_record->set_second(ToString(GetMemoryUsage()));
      out_record = response->add_records();
      out_record->set_first("memory_capacity");
      out_record->set_second(ToString(GetMemoryCapacity()));
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
    const Status status = dbm.Get(request->key(), request->omit_value() ? nullptr : &value);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_value(value);
    }
    return grpc::Status::OK;
  }

  grpc::Status GetMulti(
      grpc::ServerContext* context, const GetMultiRequest* request,
      GetMultiResponse* response) override {
    LogRequest(context, "GetMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::vector<std::string_view> keys;
    keys.reserve(request->keys_size());
    for (const auto& key : request->keys()) {
      keys.emplace_back(key);
    }
    std::map<std::string, std::string> records;
    const Status status = dbm.GetMulti(keys, &records);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    for (const auto& record : records) {
      auto* res_record = response->add_records();
      res_record->set_first(record.first);
      res_record->set_second(record.second);
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

  grpc::Status SetMulti(
      grpc::ServerContext* context, const SetMultiRequest* request,
      SetMultiResponse* response) override {
    LogRequest(context, "SetMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::map<std::string_view, std::string_view> records;
    for (const auto& record : request->records()) {
      records.emplace(std::string_view(record.first()), std::string_view(record.second()));
    }
    const Status status = dbm.SetMulti(records, request->overwrite());
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

  grpc::Status RemoveMulti(
      grpc::ServerContext* context, const RemoveMultiRequest* request,
      RemoveMultiResponse* response) override {
    LogRequest(context, "RemoveMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::vector<std::string_view> keys;
    keys.reserve(request->keys_size());
    for (const auto& key : request->keys()) {
      keys.emplace_back(key);
    }
    const Status status = dbm.RemoveMulti(keys);
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

  grpc::Status AppendMulti(
      grpc::ServerContext* context, const AppendMultiRequest* request,
      AppendMultiResponse* response) override {
    LogRequest(context, "AppendMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::map<std::string_view, std::string_view> records;
    for (const auto& record : request->records()) {
      records.emplace(std::string_view(record.first()), std::string_view(record.second()));
    }
    const Status status = dbm.AppendMulti(records, request->delim());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status CompareExchange(
      grpc::ServerContext* context, const CompareExchangeRequest* request,
      CompareExchangeResponse* response) override {
    LogRequest(context, "CompareExchange", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::string_view expected;
    if (request->expected_existence()) {
      expected = request->expected_value();
    }
    std::string_view desired;
    if (request->desired_existence()) {
      desired = request->desired_value();
    }
    const Status status = dbm.CompareExchange(request->key(), expected, desired);
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

  grpc::Status CompareExchangeMulti(
      grpc::ServerContext* context, const CompareExchangeMultiRequest* request,
      CompareExchangeMultiResponse* response) override {
    LogRequest(context, "CompareExchangeMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::vector<std::pair<std::string_view, std::string_view>> expected;
    expected.resize(request->expected_size());
    for (const auto& record : request->expected()) {
      expected.emplace_back(std::make_pair(
          std::string_view(record.key()),
          record.existence() ? std::string_view(record.value()) : std::string_view()));
    }
    std::vector<std::pair<std::string_view, std::string_view>> desired;
    desired.resize(request->desired_size());
    for (const auto& record : request->desired()) {
      desired.emplace_back(std::make_pair(
          std::string_view(record.key()),
          record.existence() ? std::string_view(record.value()) : std::string_view()));
    }
    const Status status = dbm.CompareExchangeMulti(expected, desired);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
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
      params.emplace(param.first(), param.second());
    }
    logger_->LogCat(Logger::LEVEL_INFO, "Rebuilding the database");
    const Status status = dbm.RebuildAdvanced(params);
    if (status != Status::SUCCESS) {
      logger_->LogCat(Logger::LEVEL_ERROR, "Rebuilding the database failed: ", status);
    }
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
    bool make_backup = false;
    for (const auto& param : request->params()) {
      if (param.first() == "reducer") {
        params.emplace(param.first(), param.second());
      } else if (param.first() == "make_backup") {
        make_backup = StrToBool(param.second());
      }
    }
    Status status(Status::SUCCESS);
    if (make_backup) {
      const std::string orig_path = dbm.GetFilePathSimple();
      if (orig_path.empty()) {
        status = Status(Status::INFEASIBLE_ERROR, "no file is associated");
      } else {
        struct std::tm cal;
        GetLocalCalendar(GetWallTime(), &cal);
        const std::string dest_path = orig_path + SPrintF(
            ".backup.%04d%02d%02d%2d%2d%2d",
            cal.tm_year + 1900, cal.tm_mon + 1, cal.tm_mday,
            cal.tm_hour, cal.tm_min, cal.tm_sec);
        logger_->LogCat(Logger::LEVEL_INFO, "Making a backup file: ", dest_path);
        status = dbm.CopyFileData(dest_path, request->hard());
      }
    } else {
      status = dbm.SynchronizeAdvanced(request->hard(), nullptr, params);
    }
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status Iterate(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<tkrzw::IterateResponse, tkrzw::IterateRequest>* stream) override {
    return IterateImpl(context, stream);
  }

  grpc::Status IterateImpl(grpc::ServerContext* context,
      grpc::ServerReaderWriterInterface<
                           tkrzw::IterateResponse, tkrzw::IterateRequest>* stream) {
    std::unique_ptr<DBM::Iterator> iter;
    int32_t dbm_index = -1;
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      tkrzw::IterateRequest request;
      if (!stream->Read(&request)) {
        break;
      }
      LogRequest(context, "Iterate", &request);
      if (iter == nullptr || request.dbm_index() != dbm_index) {
        if (request.dbm_index() < 0 ||
            request.dbm_index() >= static_cast<int32_t>(dbms_.size())) {
          return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
        }
        auto& dbm = *dbms_[request.dbm_index()];
        iter = dbm.MakeIterator();
        dbm_index = request.dbm_index();
      }
      tkrzw::IterateResponse response;
      switch (request.operation()) {
        case IterateRequest::OP_NONE: {
          break;
        }
        case IterateRequest::OP_FIRST: {
          const Status status = iter->First();
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_LAST: {
          const Status status = iter->Last();
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_JUMP: {
          const Status status = iter->Jump(request.key());
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_JUMP_LOWER: {
          const Status status = iter->JumpLower(request.key(), request.jump_inclusive());
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_JUMP_UPPER: {
          const Status status = iter->JumpUpper(request.key(), request.jump_inclusive());
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_NEXT: {
          const Status status = iter->Next();
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_PREVIOUS: {
          const Status status = iter->Previous();
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_GET: {
          std::string key, value;
          const Status status = iter->Get(
              request.omit_key() ? nullptr : &key, request.omit_value() ? nullptr : &value);
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          if (status == Status::SUCCESS) {
            response.set_key(key);
            response.set_value(value);
          }
          break;
        }
        case IterateRequest::OP_SET: {
          const Status status = iter->Set(request.value());
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        case IterateRequest::OP_REMOVE: {
          const Status status = iter->Remove();
          response.mutable_status()->set_code(status.GetCode());
          response.mutable_status()->set_message(status.GetMessage());
          break;
        }
        default: {
          return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknown operation");
        }
      }
      if (!stream->Write(response)) {
        break;
      }
    }
    return grpc::Status::OK;
  }

 private:
  const std::vector<std::unique_ptr<ParamDBM>>& dbms_;
  Logger* logger_;
};

}  // namespace tkrzw

#endif  // _TKRZW_SERVER_IMPL_H

// END OF FILE
