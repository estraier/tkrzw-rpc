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

class DBMServiceBase {
 public:
  DBMServiceBase(const std::vector<std::unique_ptr<ParamDBM>>& dbms, Logger* logger)
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

  grpc::Status EchoImpl(
      grpc::ServerContext* context, const EchoRequest* request,
      EchoResponse* response) {
    LogRequest(context, "Echo", request);
    response->set_echo(request->message());
    return grpc::Status::OK;
  }

  grpc::Status InspectImpl(
      grpc::ServerContext* context, const InspectRequest* request,
      InspectResponse* response) {
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

  grpc::Status GetImpl(
      grpc::ServerContext* context, const GetRequest* request,
      GetResponse* response) {
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

  grpc::Status GetMultiImpl(
      grpc::ServerContext* context, const GetMultiRequest* request,
      GetMultiResponse* response) {
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

  grpc::Status SetImpl(
      grpc::ServerContext* context, const SetRequest* request,
      SetResponse* response) {
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

  grpc::Status SetMultiImpl(
      grpc::ServerContext* context, const SetMultiRequest* request,
      SetMultiResponse* response) {
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

  grpc::Status RemoveImpl(
      grpc::ServerContext* context, const RemoveRequest* request,
      RemoveResponse* response) {
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

  grpc::Status RemoveMultiImpl(
      grpc::ServerContext* context, const RemoveMultiRequest* request,
      RemoveMultiResponse* response) {
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

  grpc::Status AppendImpl(
      grpc::ServerContext* context, const AppendRequest* request,
      AppendResponse* response) {
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

  grpc::Status AppendMultiImpl(
      grpc::ServerContext* context, const AppendMultiRequest* request,
      AppendMultiResponse* response) {
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

  grpc::Status CompareExchangeImpl(
      grpc::ServerContext* context, const CompareExchangeRequest* request,
      CompareExchangeResponse* response) {
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

  grpc::Status IncrementImpl(
      grpc::ServerContext* context, const IncrementRequest* request,
      IncrementResponse* response) {
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

  grpc::Status CompareExchangeMultiImpl(
      grpc::ServerContext* context, const CompareExchangeMultiRequest* request,
      CompareExchangeMultiResponse* response) {
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

  grpc::Status CountImpl(
      grpc::ServerContext* context, const CountRequest* request,
      CountResponse* response) {
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

  grpc::Status GetFileSizeImpl(
      grpc::ServerContext* context, const GetFileSizeRequest* request,
      GetFileSizeResponse* response) {
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

  grpc::Status ClearImpl(
      grpc::ServerContext* context, const ClearRequest* request,
      ClearResponse* response) {
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

  grpc::Status RebuildImpl(
      grpc::ServerContext* context, const RebuildRequest* request,
      RebuildResponse* response) {
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

  grpc::Status ShouldBeRebuiltImpl(
      grpc::ServerContext* context, const ShouldBeRebuiltRequest* request,
      ShouldBeRebuiltResponse* response) {
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

  grpc::Status SynchronizeImpl(
      grpc::ServerContext* context, const SynchronizeRequest* request,
      SynchronizeResponse* response) {
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

  grpc::Status StreamImpl(grpc::ServerContext* context,
                          grpc::ServerReaderWriterInterface<
                          tkrzw::StreamResponse, tkrzw::StreamRequest>* stream) {
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      tkrzw::StreamRequest request;
      if (!stream->Read(&request)) {
        break;
      }
      tkrzw::StreamResponse response;
      const grpc::Status status = StreamProcessOne(context, request, &response);
      if (!status.ok()) {
        return status;
      }
      if (!request.omit_response() && !stream->Write(response)) {
        break;
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status StreamProcessOne(
      grpc::ServerContext* context,
      const tkrzw::StreamRequest& request, tkrzw::StreamResponse* response) {
    switch (request.request_oneof_case()) {
      case tkrzw::StreamRequest::kEchoRequest: {
        const grpc::Status status =
            EchoImpl(context, &request.echo_request(), response->mutable_echo_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw::StreamRequest::kGetRequest: {
        const grpc::Status status =
              GetImpl(context, &request.get_request(), response->mutable_get_response());
        if (!status.ok()) {
          return status;
          }
        break;
        }
      case tkrzw::StreamRequest::kSetRequest: {
        const grpc::Status status =
            SetImpl(context, &request.set_request(), response->mutable_set_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw::StreamRequest::kRemoveRequest: {
        const grpc::Status status =
            RemoveImpl(context, &request.remove_request(), response->mutable_remove_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      default: {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknow request");
      }
    }
    return grpc::Status::OK;
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
      tkrzw::IterateResponse response;
      const grpc::Status status = IterateProcessOne(
          &iter, &dbm_index, context, request, &response);
      if (!status.ok()) {
        return status;
      }
      if (!stream->Write(response)) {
        break;
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status IterateProcessOne(
      std::unique_ptr<DBM::Iterator>* iter, int32_t* dbm_index, grpc::ServerContext* context,
      const tkrzw::IterateRequest& request, tkrzw::IterateResponse* response) {
    LogRequest(context, "Iterate", &request);
    if (iter == nullptr || request.dbm_index() != *dbm_index) {
      if (request.dbm_index() < 0 ||
          request.dbm_index() >= static_cast<int32_t>(dbms_.size())) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
      }
      auto& dbm = *dbms_[request.dbm_index()];
      *iter = dbm.MakeIterator();
      *dbm_index = request.dbm_index();
    }
    switch (request.operation()) {
      case IterateRequest::OP_NONE: {
        break;
      }
      case IterateRequest::OP_FIRST: {
        const Status status = (*iter)->First();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_LAST: {
        const Status status = (*iter)->Last();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_JUMP: {
        const Status status = (*iter)->Jump(request.key());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_JUMP_LOWER: {
        const Status status = (*iter)->JumpLower(request.key(), request.jump_inclusive());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_JUMP_UPPER: {
        const Status status = (*iter)->JumpUpper(request.key(), request.jump_inclusive());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_NEXT: {
        const Status status = (*iter)->Next();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_PREVIOUS: {
        const Status status = (*iter)->Previous();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_GET: {
        std::string key, value;
        const Status status = (*iter)->Get(
            request.omit_key() ? nullptr : &key, request.omit_value() ? nullptr : &value);
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        if (status == Status::SUCCESS) {
          response->set_key(key);
          response->set_value(value);
        }
        break;
      }
      case IterateRequest::OP_SET: {
        const Status status = (*iter)->Set(request.value());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case IterateRequest::OP_REMOVE: {
        const Status status = (*iter)->Remove();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      default: {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknown operation");
      }
    }
    return grpc::Status::OK;
  }

 protected:
  const std::vector<std::unique_ptr<ParamDBM>>& dbms_;
  Logger* logger_;
};

class DBMServiceImpl : public DBMServiceBase, public DBMService::Service {
 public:
  DBMServiceImpl(const std::vector<std::unique_ptr<ParamDBM>>& dbms, Logger* logger)
      : DBMServiceBase(dbms, logger) {}

  grpc::Status Echo(
      grpc::ServerContext* context, const EchoRequest* request,
      EchoResponse* response) override {
    return EchoImpl(context, request, response);
  }

  grpc::Status Inspect(
      grpc::ServerContext* context, const InspectRequest* request,
      InspectResponse* response) override {
    return InspectImpl(context, request, response);
  }

  grpc::Status Get(
      grpc::ServerContext* context, const GetRequest* request,
      GetResponse* response) override {
    return GetImpl(context, request, response);
  }

  grpc::Status GetMulti(
      grpc::ServerContext* context, const GetMultiRequest* request,
      GetMultiResponse* response) override {
    return GetMultiImpl(context, request, response);
  }

  grpc::Status Set(
      grpc::ServerContext* context, const SetRequest* request,
      SetResponse* response) override {
    return SetImpl(context, request, response);
  }

  grpc::Status SetMulti(
      grpc::ServerContext* context, const SetMultiRequest* request,
      SetMultiResponse* response) override {
    return SetMultiImpl(context, request, response);
  }

  grpc::Status Remove(
      grpc::ServerContext* context, const RemoveRequest* request,
      RemoveResponse* response) override {
    return RemoveImpl(context, request, response);
  }

  grpc::Status RemoveMulti(
      grpc::ServerContext* context, const RemoveMultiRequest* request,
      RemoveMultiResponse* response) override {
    return RemoveMultiImpl(context, request, response);
  }

  grpc::Status Append(
      grpc::ServerContext* context, const AppendRequest* request,
      AppendResponse* response) override {
    return AppendImpl(context, request, response);
  }

  grpc::Status AppendMulti(
      grpc::ServerContext* context, const AppendMultiRequest* request,
      AppendMultiResponse* response) override {
    return AppendMultiImpl(context, request, response);
  }

  grpc::Status CompareExchange(
      grpc::ServerContext* context, const CompareExchangeRequest* request,
      CompareExchangeResponse* response) override {
    return CompareExchangeImpl(context, request, response);
  }

  grpc::Status Increment(
      grpc::ServerContext* context, const IncrementRequest* request,
      IncrementResponse* response) override {
    return IncrementImpl(context, request, response);
  }

  grpc::Status CompareExchangeMulti(
      grpc::ServerContext* context, const CompareExchangeMultiRequest* request,
      CompareExchangeMultiResponse* response) override {
    return CompareExchangeMultiImpl(context, request, response);
  }

  grpc::Status Count(
      grpc::ServerContext* context, const CountRequest* request,
      CountResponse* response) override {
    return CountImpl(context, request, response);
  }

  grpc::Status GetFileSize(
      grpc::ServerContext* context, const GetFileSizeRequest* request,
      GetFileSizeResponse* response) override {
    return GetFileSizeImpl(context, request, response);
  }

  grpc::Status Clear(
      grpc::ServerContext* context, const ClearRequest* request,
      ClearResponse* response) override {
    return ClearImpl(context, request, response);
  }

  grpc::Status Rebuild(
      grpc::ServerContext* context, const RebuildRequest* request,
      RebuildResponse* response) override {
    return RebuildImpl(context, request, response);
  }

  grpc::Status ShouldBeRebuilt(
      grpc::ServerContext* context, const ShouldBeRebuiltRequest* request,
      ShouldBeRebuiltResponse* response) override {
    return ShouldBeRebuiltImpl(context, request, response);
  }

  grpc::Status Synchronize(
      grpc::ServerContext* context, const SynchronizeRequest* request,
      SynchronizeResponse* response) override {
    return SynchronizeImpl(context, request, response);
  }

  grpc::Status Stream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<tkrzw::StreamResponse, tkrzw::StreamRequest>* stream) override {
    return StreamImpl(context, stream);
  }

  grpc::Status Iterate(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<tkrzw::IterateResponse, tkrzw::IterateRequest>* stream) override {
    return IterateImpl(context, stream);
  }
};

class DBMAsyncServiceImpl : public DBMServiceBase, public DBMService::AsyncService {
 public:
  DBMAsyncServiceImpl(const std::vector<std::unique_ptr<ParamDBM>>& dbms, Logger* logger)
      : DBMServiceBase(dbms, logger) {}

  void OperateQueue(grpc::ServerCompletionQueue* queue, const bool* is_shutdown);
  void ShutdownQueue(grpc::ServerCompletionQueue* queue);
};

class AsyncDBMProcessorInterface {
 public:
  virtual ~AsyncDBMProcessorInterface() = default;
  virtual void Proceed() = 0;
  virtual void Cancel() = 0;
};

template<typename REQUEST, typename RESPONSE>
class AsyncDBMProcessor : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, PROCESS, FINISH};
  typedef void (DBMService::AsyncService::*RequestCall)(
      grpc::ServerContext*, REQUEST*, grpc::ServerAsyncResponseWriter<RESPONSE>*,
      grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);
  typedef grpc::Status (DBMServiceBase::*Call)(
      grpc::ServerContext*, const REQUEST*, RESPONSE*);

  AsyncDBMProcessor(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue,
      RequestCall request_call, Call call)
      : service_(service), queue_(queue), request_call_(request_call), call_(call),
        context_(), responder_(&context_), proc_state_(CREATE), rpc_status_(grpc::Status::OK) {
    Proceed();
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = PROCESS;
      (service_->*request_call_)(&context_, &request_, &responder_, queue_, queue_, this);
    } else if (proc_state_ == PROCESS) {
      new AsyncDBMProcessor<REQUEST, RESPONSE>(service_, queue_, request_call_, call_);
      rpc_status_ = (service_->*call_)(&context_, &request_, &response_);
      proc_state_ = FINISH;
      responder_.Finish(response_, rpc_status_, this);
    } else {
      delete this;
    }
  }

  void Cancel() override {
    if (proc_state_ == PROCESS) {
      proc_state_ = FINISH;;
      responder_.Finish(response_, rpc_status_, this);
    } else {
      delete this;
    }
  }

 private:
  DBMAsyncServiceImpl* service_;
  grpc::ServerCompletionQueue* queue_;
  RequestCall request_call_;
  Call call_;
  grpc::ServerContext context_;
  REQUEST request_;
  RESPONSE response_;
  grpc::ServerAsyncResponseWriter<RESPONSE> responder_;
  ProcState proc_state_;
  grpc::Status rpc_status_;
};

class AsyncDBMProcessorStream : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, BEGIN, READ, WRITE, FINISH};

  AsyncDBMProcessorStream(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : service_(service), queue_(queue),
        context_(), stream_(&context_), proc_state_(CREATE),
        rpc_status_(grpc::Status::OK) {
    Proceed();
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = BEGIN;
      service_->RequestStream(&context_, &stream_, queue_, queue_, this);
    } else if (proc_state_ == BEGIN || proc_state_ == READ) {
      if (proc_state_ == BEGIN) {
        new AsyncDBMProcessorStream(service_, queue_);
      }
      proc_state_ = WRITE;
      request_.Clear();
      stream_.Read(&request_, this);
    } else if (proc_state_ == WRITE) {
      response_.Clear();
      rpc_status_ = service_->StreamProcessOne(&context_, request_, &response_);
      if (rpc_status_.ok()) {
        if (request_.omit_response()) {
          proc_state_ = WRITE;
          request_.Clear();
          stream_.Read(&request_, this);
        } else {
          proc_state_ = READ;
          stream_.Write(response_, this);
        }
      } else {
        proc_state_ = FINISH;;
        stream_.Finish(rpc_status_, this);
      }
    } else {
      delete this;
    }
  }

  void Cancel() override {
    if (proc_state_ == READ || proc_state_ == WRITE) {
      proc_state_ = FINISH;;
      stream_.Finish(rpc_status_, this);
    } else {
      delete this;
    }
  }

 private:
  DBMAsyncServiceImpl* service_;
  grpc::ServerCompletionQueue* queue_;
  grpc::ServerContext context_;
  grpc::ServerAsyncReaderWriter<StreamResponse, StreamRequest> stream_;
  ProcState proc_state_;
  tkrzw::StreamRequest request_;
  tkrzw::StreamResponse response_;
  grpc::Status rpc_status_;
};

class AsyncDBMProcessorIterate : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, BEGIN, READ, WRITE, FINISH};

  AsyncDBMProcessorIterate(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : service_(service), queue_(queue),
        context_(), stream_(&context_), proc_state_(CREATE),
        iter_(nullptr), dbm_index_(-1), rpc_status_(grpc::Status::OK) {
    Proceed();
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = BEGIN;
      service_->RequestIterate(&context_, &stream_, queue_, queue_, this);
    } else if (proc_state_ == BEGIN || proc_state_ == READ) {
      if (proc_state_ == BEGIN) {
        new AsyncDBMProcessorIterate(service_, queue_);
      }
      proc_state_ = WRITE;
      request_.Clear();
      stream_.Read(&request_, this);
    } else if (proc_state_ == WRITE) {
      response_.Clear();
      rpc_status_ = service_->IterateProcessOne(
          &iter_, &dbm_index_, &context_, request_, &response_);
      if (rpc_status_.ok()) {
        proc_state_ = READ;
        stream_.Write(response_, this);
      } else {
        proc_state_ = FINISH;;
        stream_.Finish(rpc_status_, this);
      }
    } else {
      delete this;
    }
  }

  void Cancel() override {
    if (proc_state_ == READ || proc_state_ == WRITE) {
      proc_state_ = FINISH;;
      stream_.Finish(rpc_status_, this);
    } else {
      delete this;
    }
  }

 private:
  DBMAsyncServiceImpl* service_;
  grpc::ServerCompletionQueue* queue_;
  grpc::ServerContext context_;
  grpc::ServerAsyncReaderWriter<IterateResponse, IterateRequest> stream_;
  ProcState proc_state_;
  std::unique_ptr<DBM::Iterator> iter_;
  int32_t dbm_index_;
  tkrzw::IterateRequest request_;
  tkrzw::IterateResponse response_;
  grpc::Status rpc_status_;
};

inline void DBMAsyncServiceImpl::OperateQueue(
    grpc::ServerCompletionQueue* queue, const bool* is_shutdown) {
  logger_->Log(Logger::LEVEL_INFO, "Starting a completion queue");
  new AsyncDBMProcessor<EchoRequest, EchoResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestEcho,
      &DBMServiceBase::EchoImpl);
  new AsyncDBMProcessor<InspectRequest, InspectResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestInspect,
      &DBMServiceBase::InspectImpl);
  new AsyncDBMProcessor<GetRequest, GetResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGet,
      &DBMServiceBase::GetImpl);
  new AsyncDBMProcessor<GetMultiRequest, GetMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGetMulti,
      &DBMServiceBase::GetMultiImpl);
  new AsyncDBMProcessor<SetRequest, SetResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSet,
      &DBMServiceBase::SetImpl);
  new AsyncDBMProcessor<SetMultiRequest, SetMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSetMulti,
      &DBMServiceBase::SetMultiImpl);
  new AsyncDBMProcessor<RemoveRequest, RemoveResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRemove,
      &DBMServiceBase::RemoveImpl);
  new AsyncDBMProcessor<RemoveMultiRequest, RemoveMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRemoveMulti,
      &DBMServiceBase::RemoveMultiImpl);
  new AsyncDBMProcessor<AppendRequest, AppendResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestAppend,
      &DBMServiceBase::AppendImpl);
  new AsyncDBMProcessor<AppendMultiRequest, AppendMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestAppendMulti,
      &DBMServiceBase::AppendMultiImpl);
  new AsyncDBMProcessor<CompareExchangeRequest, CompareExchangeResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestCompareExchange,
      &DBMServiceBase::CompareExchangeImpl);
  new AsyncDBMProcessor<IncrementRequest, IncrementResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestIncrement,
      &DBMServiceBase::IncrementImpl);
  new AsyncDBMProcessor<CompareExchangeMultiRequest, CompareExchangeMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestCompareExchangeMulti,
      &DBMServiceBase::CompareExchangeMultiImpl);
  new AsyncDBMProcessor<CountRequest, CountResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestCount,
      &DBMServiceBase::CountImpl);
  new AsyncDBMProcessor<GetFileSizeRequest, GetFileSizeResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGetFileSize,
      &DBMServiceBase::GetFileSizeImpl);
  new AsyncDBMProcessor<ClearRequest, ClearResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestClear,
      &DBMServiceBase::ClearImpl);
  new AsyncDBMProcessor<RebuildRequest, RebuildResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRebuild,
      &DBMServiceBase::RebuildImpl);
  new AsyncDBMProcessor<ShouldBeRebuiltRequest, ShouldBeRebuiltResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestShouldBeRebuilt,
      &DBMServiceBase::ShouldBeRebuiltImpl);
  new AsyncDBMProcessor<SynchronizeRequest, SynchronizeResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSynchronize,
      &DBMServiceBase::SynchronizeImpl);
  new AsyncDBMProcessorStream(this, queue);
  new AsyncDBMProcessorIterate(this, queue);
  while (true) {
    void* tag = nullptr;
    bool ok = false;
    if (!queue->Next(&tag, &ok)) {
      break;
    }
    if (tag == nullptr) {
      continue;
    }
    auto* proc = static_cast<AsyncDBMProcessorInterface*>(tag);
    if (ok) {
      proc->Proceed();
    } else {
      proc->Cancel();
      if (*is_shutdown) {
        break;
      }
    }
  }
  logger_->Log(Logger::LEVEL_INFO, "Finishing a completion queue");
}

void DBMAsyncServiceImpl::ShutdownQueue(grpc::ServerCompletionQueue* queue) {
  queue->Shutdown();
  void* tag = nullptr;
  bool ok = false;
  while (queue->Next(&tag, &ok)) {
    if (tag == nullptr) {
      continue;
    }
    auto* proc = static_cast<AsyncDBMProcessorInterface*>(tag);
    delete proc;
  }
}

}  // namespace tkrzw

#endif  // _TKRZW_SERVER_IMPL_H

// END OF FILE
