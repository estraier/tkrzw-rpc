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

#include <sys/types.h>
#include <unistd.h>

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
#include <grpcpp/alarm.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm_remote.h"
#include "tkrzw_rpc_common.h"
#include "tkrzw_rpc.grpc.pb.h"
#include "tkrzw_rpc.pb.h"

namespace tkrzw {

static constexpr int64_t TIMESTAMP_FILE_SYNC_FREQ = 1000;
static constexpr double MAX_WAIT_TIME = 5.0;

struct ReplicationParameters {
  std::string master;
  int64_t min_timestamp;
  double wait_time;
  std::string ts_file;
  ReplicationParameters(
      const std::string& master = "", int64_t min_timestamp = 0,
      double wait_time = 0, const std::string& ts_file = "")
      : master(master), min_timestamp(min_timestamp),
        wait_time(wait_time), ts_file(ts_file) {}
};

class DBMServiceBase {
 public:
  DBMServiceBase(
      const std::vector<std::unique_ptr<ParamDBM>>& dbms,
      Logger* logger, int32_t server_id, MessageQueue* mq,
      const ReplicationParameters& repl_params = {})
      : num_active_calls_(0), first_signal_brokers_(), key_signal_brokers_(),
        dbms_(dbms), logger_(logger), server_id_(server_id),
        start_time_(GetWallTime()), num_standby_calls_(0),
        mq_(mq), repl_params_(repl_params), thread_repl_manager_(),
        repl_max_timestamp_(0), repl_alive_(false), refresh_repl_manager_(true), mutex_() {
    first_signal_brokers_.resize(dbms_.size());
    key_signal_brokers_.resize(dbms_.size());
    for (size_t i = 0; i < dbms_.size(); i++) {
      first_signal_brokers_[i] = std::make_unique<SignalBroker>();
      key_signal_brokers_[i] = std::make_unique<SlottedKeySignalBroker<std::string>>(8);
    }
  }

  virtual ~DBMServiceBase() = default;

  void StartReplication() {
    repl_alive_.store(true);
    thread_repl_manager_ = std::thread([&]{ ManageReplication(); });
  }

  void StopReplication() {
    repl_alive_.store(false);
    thread_repl_manager_.join();
  }

  void ManageReplication() {
    logger_->Log(Logger::LEVEL_DEBUG, "Starting the replication manager");
    ReplicationParameters params = repl_params_;
    bool success = true;
    while (repl_alive_.load()) {
      SleepThread(1.0);
      {
        std::lock_guard<SpinMutex> lock(mutex_);
        if (repl_params_.master.empty()) {
          continue;
        }
        if (refresh_repl_manager_) {
          refresh_repl_manager_.store(false);
          params = repl_params_;
          logger_->LogCat(Logger::LEVEL_INFO, "Replicating ", params.master,
                          " with the min_timestamp ", params.min_timestamp);
          success = true;
        }
      }
      success = DoReplicationSession(params, success);
    }
    logger_->Log(Logger::LEVEL_DEBUG, "The replicatin manager finished");
  }

  void SaveTimestamp(const std::string& path) {
    const Status status = WriteFileAtomic(path, ToString(repl_max_timestamp_.load()) + "\n");
    if (status != Status::SUCCESS) {
      logger_->LogCat(Logger::LEVEL_ERROR, "unable to save the timestamp: ", status);
    }
  }

  bool DoReplicationSession(const ReplicationParameters& params, bool success) {
    RemoteDBM master;
    Status status = master.Connect(params.master);
    if (status != Status::SUCCESS) {
      if (success) {
        logger_->Log(Logger::LEVEL_WARN, "unable to reach the master");
      }
      return false;
    }
    if (!success) {
      logger_->LogCat(Logger::LEVEL_INFO, "Reconnected to ", params.master,
                      " with the min_timestamp ", params.min_timestamp);
    }
    auto repl = master.MakeReplicator();
    status = repl->Start(params.min_timestamp, server_id_, params.wait_time);
    if (status != Status::SUCCESS) {
      logger_->LogCat(Logger::LEVEL_WARN, "replication error: ", status);
      return false;
    }
    const int32_t master_id = repl->GetMasterServerID();
    RemoteDBM::ReplicateLog op;
    int64_t count = 0;
    while (repl_alive_.load() && !refresh_repl_manager_.load()) {
      int64_t timestamp = 0;
      status = repl->Read(&timestamp, &op);
      if (count == 0 && logger_->CheckLevel(Logger::LEVEL_INFO)) {
        const double diff = GetWallTime() - timestamp / 1000.0;
        std::string ts_expr = MakeRelativeTimeExpr(diff);
        logger_->LogCat(Logger::LEVEL_INFO, "replication start: master_id=", master_id,
                        ", timestamp=", timestamp, " (", ts_expr, ")");
      }
      if (status == Status::SUCCESS) {
        if (op.dbm_index < 0 || op.dbm_index >= static_cast<int32_t>(dbms_.size())) {
          logger_->LogCat(Logger::LEVEL_ERROR, "out-of-range DBM index");
          return true;
        }
        if (op.server_id == server_id_) {
          logger_->LogCat(Logger::LEVEL_ERROR, "duplicated server ID");
          return true;
        }
        DBM* dbm = dbms_[op.dbm_index].get();
        repl_max_timestamp_.store(std::max(timestamp, repl_max_timestamp_.load()));
        switch (op.op_type) {
          case DBMUpdateLoggerMQ::OP_SET: {
            logger_->LogCat(Logger::LEVEL_DEBUG, "replication: ts=", timestamp,
                            ", server_id=", op.server_id, ", dbm_index=", op.dbm_index,
                            ", op=SET");
            DBMUpdateLoggerMQ::OverwriteThreadServerID(master_id);
            status = dbm->Set(op.key, op.value);
            DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
            if (status != Status::SUCCESS) {
              logger_->LogCat(Logger::LEVEL_ERROR, "Set failed: ", status);
              return true;
            }
            break;
          }
          case DBMUpdateLoggerMQ::OP_REMOVE: {
            logger_->LogCat(Logger::LEVEL_DEBUG, "replication: ts=", timestamp,
                            ", server_id=", op.server_id, ", dbm_index=", op.dbm_index,
                            ", op=REMOVE");
            DBMUpdateLoggerMQ::OverwriteThreadServerID(master_id);
            status = dbm->Remove(op.key);
            DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
            if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
              logger_->LogCat(Logger::LEVEL_ERROR, "Remove failed: ", status);
              return true;
            }
            break;
          }
          case DBMUpdateLoggerMQ::OP_CLEAR: {
            logger_->LogCat(Logger::LEVEL_DEBUG, "replication: ts=", timestamp,
                            ", server_id=", op.server_id, ", dbm_index=", op.dbm_index,
                            ", op=CLEAR");
            DBMUpdateLoggerMQ::OverwriteThreadServerID(master_id);
            status = dbm->Clear();
            DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
            if (status != Status::SUCCESS) {
              logger_->LogCat(Logger::LEVEL_ERROR, "Clear failed: ", status);
              return true;
            }
            break;
          }
          default:
            break;
        }
      } else if (status == Status::INFEASIBLE_ERROR) {
        repl_max_timestamp_.store(std::max(timestamp, repl_max_timestamp_.load()));
      } else {
        logger_->LogCat(Logger::LEVEL_WARN, "replication error: ", status);
        break;
      }
      if (count % TIMESTAMP_FILE_SYNC_FREQ == 0 && !params.ts_file.empty()) {
        SaveTimestamp(params.ts_file);
      }
      count++;
    }
    if (!params.ts_file.empty()) {
      SaveTimestamp(params.ts_file);
    }
    return true;
  }

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
      grpc::ServerContext* context, const tkrzw_rpc::EchoRequest* request,
      tkrzw_rpc::EchoResponse* response) {
    LogRequest(context, "Echo", request);
    response->mutable_status()->set_code(Status::SUCCESS);
    response->set_echo(request->message());
    return grpc::Status::OK;
  }

  grpc::Status InspectImpl(
      grpc::ServerContext* context, const tkrzw_rpc::InspectRequest* request,
      tkrzw_rpc::InspectResponse* response) {
    LogRequest(context, "Inspect", request);
    if (request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    response->mutable_status()->set_code(Status::SUCCESS);
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
      out_record->set_first("process_id");
      out_record->set_second(ToString(getpid()));
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
      out_record = response->add_records();
      out_record->set_first("repl_max_timestamp");
      out_record->set_second(ToString(repl_max_timestamp_.load()));
      out_record = response->add_records();
      out_record->set_first("num_active_calls");
      out_record->set_second(ToString(num_active_calls_.load() - num_standby_calls_));
      const double current_time = GetWallTime();
      out_record = response->add_records();
      out_record->set_first("current_time");
      out_record->set_second(SPrintF("%.3f", current_time));
      out_record = response->add_records();
      out_record->set_first("running_time");
      out_record->set_second(SPrintF("%.3f", current_time - start_time_));
    }
    return grpc::Status::OK;
  }

  grpc::Status GetImpl(
      grpc::ServerContext* context, const tkrzw_rpc::GetRequest* request,
      tkrzw_rpc::GetResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::GetMultiRequest* request,
      tkrzw_rpc::GetMultiResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::SetRequest* request,
      tkrzw_rpc::SetResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::SetMultiRequest* request,
      tkrzw_rpc::SetMultiResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::RemoveRequest* request,
      tkrzw_rpc::RemoveResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::RemoveMultiRequest* request,
      tkrzw_rpc::RemoveMultiResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::AppendRequest* request,
      tkrzw_rpc::AppendResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::AppendMultiRequest* request,
      tkrzw_rpc::AppendMultiResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::CompareExchangeRequest* request,
      tkrzw_rpc::CompareExchangeResponse* response) {
    LogRequest(context, "CompareExchange", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::string_view expected;
    if (request->expected_existence()) {
      if (request->expect_any_value()) {
        expected = DBM::ANY_DATA;
      } else {
        expected = request->expected_value();
      }
    }
    std::string_view desired;
    if (request->desire_no_update()) {
      desired = DBM::ANY_DATA;
    } else if (request->desired_existence()) {
      desired = request->desired_value();
    }
    std::string actual;
    bool found = false;
    const Status status = dbm.CompareExchange(
        request->key(), expected, desired, request->get_actual() ? &actual : nullptr, &found);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (!actual.empty()) {
      response->set_actual(actual);
    }
    response->set_found(found);
    return grpc::Status::OK;
  }

  grpc::Status IncrementImpl(
      grpc::ServerContext* context, const tkrzw_rpc::IncrementRequest* request,
      tkrzw_rpc::IncrementResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::CompareExchangeMultiRequest* request,
      tkrzw_rpc::CompareExchangeMultiResponse* response) {
    LogRequest(context, "CompareExchangeMulti", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::vector<std::pair<std::string_view, std::string_view>> expected;
    expected.resize(request->expected_size());
    for (const auto& record : request->expected()) {
      std::string_view value;
      if (record.existence()) {
        if (record.any_value()) {
          value = DBM::ANY_DATA;
        } else {
          value = record.value();
        }
      }
      expected.emplace_back(std::make_pair(std::string_view(record.key()), value));
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

  grpc::Status RekeyImpl(
      grpc::ServerContext* context, const tkrzw_rpc::RekeyRequest* request,
      tkrzw_rpc::RekeyResponse* response) {
    LogRequest(context, "Rekey", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.Rekey(
        request->old_key(), request->new_key(), request->overwrite(), request->copying());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status PopFirstImpl(
      grpc::ServerContext* context, const tkrzw_rpc::PopFirstRequest* request,
      tkrzw_rpc::PopFirstResponse* response) {
    LogRequest(context, "PopFirst", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::string key, value;
    const Status status = dbm.PopFirst(request->omit_key() ? nullptr : &key,
                                       request->omit_value() ? nullptr : &value);
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    if (status == Status::SUCCESS) {
      response->set_key(key);
      response->set_value(value);
    }
    return grpc::Status::OK;
  }

  grpc::Status PushLastImpl(
      grpc::ServerContext* context, const tkrzw_rpc::PushLastRequest* request,
      tkrzw_rpc::PushLastResponse* response) {
    LogRequest(context, "PushLast", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    const Status status = dbm.PushLast(request->value(), request->wtime());
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status PushLastAndNotify(
      grpc::ServerContext* context, const tkrzw_rpc::PushLastRequest* request,
      tkrzw_rpc::PushLastResponse* response) {
    const grpc::Status status = PushLastImpl(context, request, response);
    if (request->notify() && status.ok() && response->status().code() == 0) {
      first_signal_brokers_[request->dbm_index()]->Send();
    }
    return status;
  }

  grpc::Status CountImpl(
      grpc::ServerContext* context, const tkrzw_rpc::CountRequest* request,
      tkrzw_rpc::CountResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::GetFileSizeRequest* request,
      tkrzw_rpc::GetFileSizeResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::ClearRequest* request,
      tkrzw_rpc::ClearResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::RebuildRequest* request,
      tkrzw_rpc::RebuildResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::ShouldBeRebuiltRequest* request,
      tkrzw_rpc::ShouldBeRebuiltResponse* response) {
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
      grpc::ServerContext* context, const tkrzw_rpc::SynchronizeRequest* request,
      tkrzw_rpc::SynchronizeResponse* response) {
    LogRequest(context, "Synchronize", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::map<std::string, std::string> params;
    bool make_backup = false;
    std::string backup_suffix;
    for (const auto& param : request->params()) {
      if (param.first() == "reducer") {
        params.emplace(param.first(), param.second());
      } else if (param.first() == "make_backup") {
        make_backup = true;
        backup_suffix = param.second();
      }
    }
    Status status(Status::SUCCESS);
    if (make_backup) {
      const std::string orig_path = dbm.GetFilePathSimple();
      if (orig_path.empty()) {
        status = Status(Status::INFEASIBLE_ERROR, "no file is associated");
      } else {
        backup_suffix = StrReplaceRegex(backup_suffix, "[^-_.0-9a-zA-Z]", "");
        if (backup_suffix.empty()) {
          struct std::tm cal;
          GetUniversalCalendar(GetWallTime(), &cal);
          backup_suffix = SPrintF("%04d%02d%02d%2d%2d%2d",
                                  cal.tm_year + 1900, cal.tm_mon + 1, cal.tm_mday,
                                  cal.tm_hour, cal.tm_min, cal.tm_sec);
        }
        const std::string dest_path = orig_path + ".backup." + backup_suffix;
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

  grpc::Status SearchImpl(
      grpc::ServerContext* context, const tkrzw_rpc::SearchRequest* request,
      tkrzw_rpc::SearchResponse* response) {
    LogRequest(context, "Search", request);
    if (request->dbm_index() < 0 || request->dbm_index() >= static_cast<int32_t>(dbms_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    auto& dbm = *dbms_[request->dbm_index()];
    std::vector<std::string> matched;
    const Status status =
        SearchDBMModal(&dbm, request->mode(), request->pattern(), &matched, request->capacity());
    if (status == Status::SUCCESS) {
      for (const auto& key : matched) {
        response->add_matched(key);
      }
    }
    response->mutable_status()->set_code(status.GetCode());
    response->mutable_status()->set_message(status.GetMessage());
    return grpc::Status::OK;
  }

  grpc::Status StreamImpl(grpc::ServerContext* context,
                          grpc::ServerReaderWriterInterface<
                          tkrzw_rpc::StreamResponse, tkrzw_rpc::StreamRequest>* stream) {
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      tkrzw_rpc::StreamRequest request;
      if (!stream->Read(&request)) {
        break;
      }
      tkrzw_rpc::StreamResponse response;
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
      const tkrzw_rpc::StreamRequest& request, tkrzw_rpc::StreamResponse* response) {
    switch (request.request_oneof_case()) {
      case tkrzw_rpc::StreamRequest::kEchoRequest: {
        const grpc::Status status =
            EchoImpl(context, &request.echo_request(), response->mutable_echo_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw_rpc::StreamRequest::kGetRequest: {
        const grpc::Status status =
              GetImpl(context, &request.get_request(), response->mutable_get_response());
        if (!status.ok()) {
          return status;
          }
        break;
        }
      case tkrzw_rpc::StreamRequest::kSetRequest: {
        const grpc::Status status =
            SetImpl(context, &request.set_request(), response->mutable_set_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw_rpc::StreamRequest::kRemoveRequest: {
        const grpc::Status status =
            RemoveImpl(context, &request.remove_request(), response->mutable_remove_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw_rpc::StreamRequest::kAppendRequest: {
        const grpc::Status status =
            AppendImpl(context, &request.append_request(), response->mutable_append_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw_rpc::StreamRequest::kCompareExchangeRequest: {
        const grpc::Status status =
            CompareExchangeImpl(context, &request.compare_exchange_request(),
                                response->mutable_compare_exchange_response());
        if (!status.ok()) {
          return status;
        }
        break;
      }
      case tkrzw_rpc::StreamRequest::kIncrementRequest: {
        const grpc::Status status =
            IncrementImpl(context, &request.increment_request(),
                          response->mutable_increment_response());
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
                           tkrzw_rpc::IterateResponse, tkrzw_rpc::IterateRequest>* stream) {
    std::unique_ptr<DBM::Iterator> iter;
    int32_t dbm_index = -1;
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      tkrzw_rpc::IterateRequest request;
      if (!stream->Read(&request)) {
        break;
      }
      tkrzw_rpc::IterateResponse response;
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
      const tkrzw_rpc::IterateRequest& request, tkrzw_rpc::IterateResponse* response) {
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
      case tkrzw_rpc::IterateRequest::OP_NONE: {
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_FIRST: {
        const Status status = (*iter)->First();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_LAST: {
        const Status status = (*iter)->Last();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_JUMP: {
        const Status status = (*iter)->Jump(request.key());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_JUMP_LOWER: {
        const Status status = (*iter)->JumpLower(request.key(), request.jump_inclusive());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_JUMP_UPPER: {
        const Status status = (*iter)->JumpUpper(request.key(), request.jump_inclusive());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_NEXT: {
        const Status status = (*iter)->Next();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_PREVIOUS: {
        const Status status = (*iter)->Previous();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_GET: {
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
      case tkrzw_rpc::IterateRequest::OP_SET: {
        const Status status = (*iter)->Set(request.value());
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_REMOVE: {
        const Status status = (*iter)->Remove();
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        break;
      }
      case tkrzw_rpc::IterateRequest::OP_STEP: {
        std::string key, value;
        const Status status = (*iter)->Step(
            request.omit_key() ? nullptr : &key, request.omit_value() ? nullptr : &value);
        response->mutable_status()->set_code(status.GetCode());
        response->mutable_status()->set_message(status.GetMessage());
        if (status == Status::SUCCESS) {
          response->set_key(key);
          response->set_value(value);
        }
        break;
      }
      default: {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknown operation");
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status ReplicateImpl(
      grpc::ServerContext* context, const tkrzw_rpc::ReplicateRequest* request,
      grpc::ServerWriterInterface<tkrzw_rpc::ReplicateResponse>* writer) {
    std::unique_ptr<MessageQueue::Reader> reader;
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      tkrzw_rpc::ReplicateResponse response;
      const grpc::Status status = ReplicateProcessOne(&reader, context, *request, &response);
      if (!status.ok()) {
        return status;
      }
      if (!writer->Write(response)) {
        break;
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status ReplicateProcessOne(
      std::unique_ptr<MessageQueue::Reader>* reader, grpc::ServerContext* context,
      const tkrzw_rpc::ReplicateRequest& request, tkrzw_rpc::ReplicateResponse* response) {
    if (*reader == nullptr) {
      LogRequest(context, "Replicate", &request);
      if (mq_ == nullptr) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "disabled update logging");
      }
      if (request.server_id() == server_id_) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "self server ID");
      }
      *reader = mq_->MakeReader(request.min_timestamp());
      response->set_op_type(tkrzw_rpc::ReplicateResponse::OP_NOOP);
      response->set_server_id(server_id_);
      return grpc::Status::OK;
    }
    int64_t timestamp = 0;
    std::string message;
    double wait_time = request.wait_time();
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      if (wait_time <= 0) {
        mq_->UpdateTimestamp(-1);
      }
      Status status = (*reader)->Read(&timestamp, &message, wait_time);
      if (status == Status::SUCCESS) {
        response->set_timestamp(timestamp);
        DBMUpdateLoggerMQ::UpdateLog op;
        status = DBMUpdateLoggerMQ::ParseUpdateLog(message, &op);
        if (status == Status::SUCCESS) {
          if (op.server_id == request.server_id()) {
            continue;
          }
          switch (op.op_type) {
            case DBMUpdateLoggerMQ::OP_SET:
              response->set_op_type(tkrzw_rpc::ReplicateResponse::OP_SET);
              break;
            case DBMUpdateLoggerMQ::OP_REMOVE:
              response->set_op_type(tkrzw_rpc::ReplicateResponse::OP_REMOVE);
              break;
            case DBMUpdateLoggerMQ::OP_CLEAR:
              response->set_op_type(tkrzw_rpc::ReplicateResponse::OP_CLEAR);
              break;
            default:
              break;
          }
          response->set_server_id(op.server_id);
          response->set_dbm_index(op.dbm_index);
          response->set_key(op.key.data(), op.key.size());
          response->set_value(op.value.data(), op.value.size());
        }
      } else if (status == Status::INFEASIBLE_ERROR) {
        if (wait_time > 0) {
          wait_time = 0;
          continue;
        }
        response->set_timestamp(timestamp);
      }
      response->mutable_status()->set_code(status.GetCode());
      response->mutable_status()->set_message(status.GetMessage());
      break;
    }
    return grpc::Status::OK;
  }

  grpc::Status ChangeMasterImpl(
      grpc::ServerContext* context, const tkrzw_rpc::ChangeMasterRequest* request,
      tkrzw_rpc::ChangeMasterResponse* response) {
    LogRequest(context, "ChangeMaster", request);
    std::lock_guard<SpinMutex> lock(mutex_);
    repl_params_.master = request->master();
    repl_params_.min_timestamp =
        std::max<int64_t>(0, repl_max_timestamp_.load() + request->timestamp_skew());
    repl_max_timestamp_.store(repl_params_.min_timestamp);
    refresh_repl_manager_.store(true);
    return grpc::Status::OK;
  }

  std::atomic_int32_t num_active_calls_;
  std::vector<std::unique_ptr<SignalBroker>> first_signal_brokers_;
  std::vector<std::unique_ptr<SlottedKeySignalBroker<std::string>>> key_signal_brokers_;

 protected:
  const std::vector<std::unique_ptr<ParamDBM>>& dbms_;
  Logger* logger_;
  int32_t server_id_;
  double start_time_;
  int32_t num_standby_calls_;
  MessageQueue* mq_;
  ReplicationParameters repl_params_;
  std::thread thread_repl_manager_;
  std::atomic_int64_t repl_max_timestamp_;
  std::atomic_bool repl_alive_;
  std::atomic_bool refresh_repl_manager_;
  SpinMutex mutex_;
};

class DBMServiceImpl : public DBMServiceBase, public tkrzw_rpc::DBMService::Service {
 public:
  DBMServiceImpl(
      const std::vector<std::unique_ptr<ParamDBM>>& dbms,
      Logger* logger, int32_t server_id, MessageQueue* mq,
      const ReplicationParameters& repl_params = {})
      : DBMServiceBase(dbms, logger, server_id, mq, repl_params) {}

  grpc::Status Echo(
      grpc::ServerContext* context, const tkrzw_rpc::EchoRequest* request,
      tkrzw_rpc::EchoResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return EchoImpl(context, request, response);
  }

  grpc::Status Inspect(
      grpc::ServerContext* context, const tkrzw_rpc::InspectRequest* request,
      tkrzw_rpc::InspectResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return InspectImpl(context, request, response);
  }

  grpc::Status Get(
      grpc::ServerContext* context, const tkrzw_rpc::GetRequest* request,
      tkrzw_rpc::GetResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return GetImpl(context, request, response);
  }

  grpc::Status GetMulti(
      grpc::ServerContext* context, const tkrzw_rpc::GetMultiRequest* request,
      tkrzw_rpc::GetMultiResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return GetMultiImpl(context, request, response);
  }

  grpc::Status Set(
      grpc::ServerContext* context, const tkrzw_rpc::SetRequest* request,
      tkrzw_rpc::SetResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return SetImpl(context, request, response);
  }

  grpc::Status SetMulti(
      grpc::ServerContext* context, const tkrzw_rpc::SetMultiRequest* request,
      tkrzw_rpc::SetMultiResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return SetMultiImpl(context, request, response);
  }

  grpc::Status Remove(
      grpc::ServerContext* context, const tkrzw_rpc::RemoveRequest* request,
      tkrzw_rpc::RemoveResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return RemoveImpl(context, request, response);
  }

  grpc::Status RemoveMulti(
      grpc::ServerContext* context, const tkrzw_rpc::RemoveMultiRequest* request,
      tkrzw_rpc::RemoveMultiResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return RemoveMultiImpl(context, request, response);
  }

  grpc::Status Append(
      grpc::ServerContext* context, const tkrzw_rpc::AppendRequest* request,
      tkrzw_rpc::AppendResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return AppendImpl(context, request, response);
  }

  grpc::Status AppendMulti(
      grpc::ServerContext* context, const tkrzw_rpc::AppendMultiRequest* request,
      tkrzw_rpc::AppendMultiResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return AppendMultiImpl(context, request, response);
  }

  grpc::Status CompareExchange(
      grpc::ServerContext* context, const tkrzw_rpc::CompareExchangeRequest* request,
      tkrzw_rpc::CompareExchangeResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    if (request->retry_wait() <= 0) {
      const grpc::Status status = CompareExchangeImpl(context, request, response);
      if (request->notify() && status.ok() && response->status().code() == 0) {
        key_signal_brokers_[request->dbm_index()]->Send(request->key());
      }
      return status;
    }
    if (request->dbm_index() >= static_cast<int32_t>(key_signal_brokers_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    const double deadline = GetWallTime() + request->retry_wait();
    grpc::Status status = grpc::Status::OK;
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      SlottedKeySignalBroker<std::string>::Waiter waiter(
          key_signal_brokers_[request->dbm_index()].get(), request->key());
      status = CompareExchangeImpl(context, request, response);
      if (!status.ok() || response->status().code() != Status::INFEASIBLE_ERROR) {
        break;
      }
      const double time_diff = deadline - GetWallTime();
      if (time_diff <= 0) {
        break;
      }
      waiter.Wait(std::min(MAX_WAIT_TIME, time_diff));
      response->Clear();
    }
    if (request->notify() && status.ok() && response->status().code() == 0) {
      key_signal_brokers_[request->dbm_index()]->Send(request->key());
    }
    return status;
  }

  grpc::Status Increment(
      grpc::ServerContext* context, const tkrzw_rpc::IncrementRequest* request,
      tkrzw_rpc::IncrementResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return IncrementImpl(context, request, response);
  }

  grpc::Status CompareExchangeMulti(
      grpc::ServerContext* context, const tkrzw_rpc::CompareExchangeMultiRequest* request,
      tkrzw_rpc::CompareExchangeMultiResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return CompareExchangeMultiImpl(context, request, response);
  }

  grpc::Status Rekey(
      grpc::ServerContext* context, const tkrzw_rpc::RekeyRequest* request,
      tkrzw_rpc::RekeyResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return RekeyImpl(context, request, response);
  }

  grpc::Status PopFirst(
      grpc::ServerContext* context, const tkrzw_rpc::PopFirstRequest* request,
      tkrzw_rpc::PopFirstResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    if (request->retry_wait() <= 0) {
      return PopFirstImpl(context, request, response);
    }
    if (request->dbm_index() >= static_cast<int32_t>(first_signal_brokers_.size())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "dbm_index is out of range");
    }
    const double deadline = GetWallTime() + request->retry_wait();
    while (true) {
      if (context->IsCancelled()) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      }
      SignalBroker::Waiter waiter(first_signal_brokers_[request->dbm_index()].get());
      const grpc::Status status = PopFirstImpl(context, request, response);
      if (!status.ok() || response->status().code() != Status::NOT_FOUND_ERROR) {
        return status;
      }
      const double time_diff = deadline - GetWallTime();
      if (time_diff <= 0) {
        break;
      }
      waiter.Wait(std::min(MAX_WAIT_TIME, time_diff));
      response->Clear();
    }
    return grpc::Status::OK;
  }

  grpc::Status PushLast(
      grpc::ServerContext* context, const tkrzw_rpc::PushLastRequest* request,
      tkrzw_rpc::PushLastResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return PushLastAndNotify(context, request, response);
  }

  grpc::Status Count(
      grpc::ServerContext* context, const tkrzw_rpc::CountRequest* request,
      tkrzw_rpc::CountResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return CountImpl(context, request, response);
  }

  grpc::Status GetFileSize(
      grpc::ServerContext* context, const tkrzw_rpc::GetFileSizeRequest* request,
      tkrzw_rpc::GetFileSizeResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return GetFileSizeImpl(context, request, response);
  }

  grpc::Status Clear(
      grpc::ServerContext* context, const tkrzw_rpc::ClearRequest* request,
      tkrzw_rpc::ClearResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return ClearImpl(context, request, response);
  }

  grpc::Status Rebuild(
      grpc::ServerContext* context, const tkrzw_rpc::RebuildRequest* request,
      tkrzw_rpc::RebuildResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return RebuildImpl(context, request, response);
  }

  grpc::Status ShouldBeRebuilt(
      grpc::ServerContext* context, const tkrzw_rpc::ShouldBeRebuiltRequest* request,
      tkrzw_rpc::ShouldBeRebuiltResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return ShouldBeRebuiltImpl(context, request, response);
  }

  grpc::Status Synchronize(
      grpc::ServerContext* context, const tkrzw_rpc::SynchronizeRequest* request,
      tkrzw_rpc::SynchronizeResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return SynchronizeImpl(context, request, response);
  }

  grpc::Status Search(
      grpc::ServerContext* context, const tkrzw_rpc::SearchRequest* request,
      tkrzw_rpc::SearchResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return SearchImpl(context, request, response);
  }

  grpc::Status Stream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<
      tkrzw_rpc::StreamResponse, tkrzw_rpc::StreamRequest>* stream) override {
    ScopedCounter sc(&num_active_calls_);
    return StreamImpl(context, stream);
  }

  grpc::Status Iterate(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<
      tkrzw_rpc::IterateResponse, tkrzw_rpc::IterateRequest>* stream) override {
    ScopedCounter sc(&num_active_calls_);
    return IterateImpl(context, stream);
  }

  grpc::Status Replicate(
      grpc::ServerContext* context, const tkrzw_rpc::ReplicateRequest* request,
      grpc::ServerWriter<tkrzw_rpc::ReplicateResponse>* writer) override {
    ScopedCounter sc(&num_active_calls_);
    return ReplicateImpl(context, request, writer);
  }

  grpc::Status ChangeMaster(
      grpc::ServerContext* context, const tkrzw_rpc::ChangeMasterRequest* request,
      tkrzw_rpc::ChangeMasterResponse* response) override {
    ScopedCounter sc(&num_active_calls_);
    return ChangeMasterImpl(context, request, response);
  }
};

class DBMAsyncServiceImpl : public DBMServiceBase, public tkrzw_rpc::DBMService::AsyncService {
 public:
  DBMAsyncServiceImpl(
      const std::vector<std::unique_ptr<ParamDBM>>& dbms,
      Logger* logger, int32_t server_id, MessageQueue* mq,
      const ReplicationParameters& repl_params = {})
      : DBMServiceBase(dbms, logger, server_id, mq, repl_params) {}

  void OperateQueue(grpc::ServerCompletionQueue* queue, const std::atomic_bool* is_shutdown);
  void ShutdownQueue(grpc::ServerCompletionQueue* queue);
};

class AsyncDBMProcessorInterface {
 public:
  explicit AsyncDBMProcessorInterface(std::atomic_int32_t* num_active_calls)
      : sc_(num_active_calls) {}
  virtual ~AsyncDBMProcessorInterface() = default;
  virtual void Proceed() = 0;
  virtual void Cancel(bool is_shutdown) = 0;

 private:
  ScopedCounter<std::atomic_int32_t> sc_;
};

template<typename REQUEST, typename RESPONSE>
class AsyncDBMProcessor : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, PROCESS, FINISH};
  typedef void (tkrzw_rpc::DBMService::AsyncService::*RequestCall)(
      grpc::ServerContext*, REQUEST*, grpc::ServerAsyncResponseWriter<RESPONSE>*,
      grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);
  typedef grpc::Status (DBMServiceBase::*Call)(
      grpc::ServerContext*, const REQUEST*, RESPONSE*);

  AsyncDBMProcessor(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue,
      RequestCall request_call, Call call)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue), request_call_(request_call), call_(call),
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

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == PROCESS) {
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

template<typename REQUEST, typename RESPONSE>
class AsyncDBMProcessorBackground : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, PROCESS, FINISH};
  typedef void (tkrzw_rpc::DBMService::AsyncService::*RequestCall)(
      grpc::ServerContext*, REQUEST*, grpc::ServerAsyncResponseWriter<RESPONSE>*,
      grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);
  typedef grpc::Status (DBMServiceBase::*Call)(
      grpc::ServerContext*, const REQUEST*, RESPONSE*);

  AsyncDBMProcessorBackground(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue,
      RequestCall request_call, Call call)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue), request_call_(request_call), call_(call),
        context_(), responder_(&context_), proc_state_(CREATE), rpc_status_(grpc::Status::OK),
        bg_thread_() {
    Proceed();
  }

  ~AsyncDBMProcessorBackground() {
    if (bg_thread_.joinable()) {
      bg_thread_.join();
    }
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = PROCESS;
      (service_->*request_call_)(&context_, &request_, &responder_, queue_, queue_, this);
    } else if (proc_state_ == PROCESS) {
      new AsyncDBMProcessorBackground<REQUEST, RESPONSE>(service_, queue_, request_call_, call_);
      auto task =
          [&]() {
            rpc_status_ = (service_->*call_)(&context_, &request_, &response_);
            proc_state_ = FINISH;
            responder_.Finish(response_, rpc_status_, this);
          };
      bg_thread_ = std::thread(task);
    } else {
      delete this;
    }
  }

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == PROCESS) {
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
  std::thread bg_thread_;
};

class AsyncDBMProcessorCompareExchange : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, PROCESS, FINISH};

  AsyncDBMProcessorCompareExchange(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue), context_(), responder_(&context_),
        proc_state_(CREATE), rpc_status_(grpc::Status::OK),
        bg_thread_(), alive_(true) {
    Proceed();
  }

  ~AsyncDBMProcessorCompareExchange() {
    alive_.store(false);
    if (bg_thread_.joinable()) {
      bg_thread_.join();
    }
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = PROCESS;
      service_->RequestCompareExchange(&context_, &request_, &responder_, queue_, queue_, this);
    } else if (proc_state_ == PROCESS) {
      new AsyncDBMProcessorCompareExchange(service_, queue_);
      rpc_status_ = service_->CompareExchangeImpl(&context_, &request_, &response_);
      if (rpc_status_.ok() && response_.status().code() == Status::INFEASIBLE_ERROR &&
          request_.retry_wait() > 0) {
        auto task =
            [&]() {
              const double deadline = GetWallTime() + request_.retry_wait();
              while (alive_.load()) {
                SlottedKeySignalBroker<std::string>::Waiter waiter(
                    service_->key_signal_brokers_[request_.dbm_index()].get(), request_.key());
                rpc_status_ = service_->CompareExchangeImpl(&context_, &request_, &response_);
                if (!rpc_status_.ok() || response_.status().code() != Status::INFEASIBLE_ERROR) {
                  break;
                }
                const double time_diff = deadline - GetWallTime();
                if (time_diff <= 0) {
                  break;
                }
                waiter.Wait(std::min(MAX_WAIT_TIME, time_diff));
                response_.Clear();
              }
              if (request_.notify() && rpc_status_.ok() && response_.status().code() == 0) {
                service_->key_signal_brokers_[request_.dbm_index()]->Send(request_.key());
              }
              proc_state_ = FINISH;
              responder_.Finish(response_, rpc_status_, this);
            };
        bg_thread_ = std::thread(task);
      } else {
        if (request_.notify() && rpc_status_.ok() && response_.status().code() == 0) {
          service_->key_signal_brokers_[request_.dbm_index()]->Send(request_.key());
        }
        proc_state_ = FINISH;
        responder_.Finish(response_, rpc_status_, this);
      }
    } else {
      delete this;
    }
  }

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == PROCESS) {
      proc_state_ = FINISH;;
      responder_.Finish(response_, rpc_status_, this);
    } else {
      delete this;
    }
  }

 private:
  DBMAsyncServiceImpl* service_;
  grpc::ServerCompletionQueue* queue_;
  grpc::ServerContext context_;
  tkrzw_rpc::CompareExchangeRequest request_;
  tkrzw_rpc::CompareExchangeResponse response_;
  grpc::ServerAsyncResponseWriter<tkrzw_rpc::CompareExchangeResponse> responder_;
  ProcState proc_state_;
  grpc::Status rpc_status_;
  std::thread bg_thread_;
  std::atomic_bool alive_;
};

class AsyncDBMProcessorPopFirst : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, PROCESS, FINISH};

  AsyncDBMProcessorPopFirst(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue), context_(), responder_(&context_),
        proc_state_(CREATE), rpc_status_(grpc::Status::OK),
        bg_thread_(), alive_(true) {
    Proceed();
  }

  ~AsyncDBMProcessorPopFirst() {
    alive_.store(false);
    if (bg_thread_.joinable()) {
      bg_thread_.join();
    }
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      proc_state_ = PROCESS;
      service_->RequestPopFirst(&context_, &request_, &responder_, queue_, queue_, this);
    } else if (proc_state_ == PROCESS) {
      new AsyncDBMProcessorPopFirst(service_, queue_);
      rpc_status_ = service_->PopFirstImpl(&context_, &request_, &response_);
      if (rpc_status_.ok() && response_.status().code() == Status::NOT_FOUND_ERROR &&
          request_.retry_wait() > 0) {
        auto task =
            [&]() {
              const double deadline = GetWallTime() + request_.retry_wait();
              while (alive_.load()) {
                SignalBroker::Waiter waiter(
                    service_->first_signal_brokers_[request_.dbm_index()].get());
                rpc_status_ = service_->PopFirstImpl(&context_, &request_, &response_);
                if (!rpc_status_.ok() || response_.status().code() != Status::NOT_FOUND_ERROR) {
                  break;
                }
                const double time_diff = deadline - GetWallTime();
                if (time_diff <= 0) {
                  break;
                }
                waiter.Wait(std::min(MAX_WAIT_TIME, time_diff));
                response_.Clear();
              }
              proc_state_ = FINISH;
              responder_.Finish(response_, rpc_status_, this);
            };
        bg_thread_ = std::thread(task);
      } else {
        proc_state_ = FINISH;
        responder_.Finish(response_, rpc_status_, this);
      }
    } else {
      delete this;
    }
  }

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == PROCESS) {
      proc_state_ = FINISH;;
      responder_.Finish(response_, rpc_status_, this);
    } else {
      delete this;
    }
  }

 private:
  DBMAsyncServiceImpl* service_;
  grpc::ServerCompletionQueue* queue_;
  grpc::ServerContext context_;
  tkrzw_rpc::PopFirstRequest request_;
  tkrzw_rpc::PopFirstResponse response_;
  grpc::ServerAsyncResponseWriter<tkrzw_rpc::PopFirstResponse> responder_;
  ProcState proc_state_;
  grpc::Status rpc_status_;
  std::thread bg_thread_;
  std::atomic_bool alive_;
};

class AsyncDBMProcessorStream : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, BEGIN, READ, WRITE, FINISH};

  AsyncDBMProcessorStream(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue),
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

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == READ || proc_state_ == WRITE) {
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
  grpc::ServerAsyncReaderWriter<tkrzw_rpc::StreamResponse, tkrzw_rpc::StreamRequest> stream_;
  ProcState proc_state_;
  tkrzw_rpc::StreamRequest request_;
  tkrzw_rpc::StreamResponse response_;
  grpc::Status rpc_status_;
};

class AsyncDBMProcessorIterate : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, BEGIN, READ, WRITE, FINISH};

  AsyncDBMProcessorIterate(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue),
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

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == READ || proc_state_ == WRITE) {
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
  grpc::ServerAsyncReaderWriter<tkrzw_rpc::IterateResponse, tkrzw_rpc::IterateRequest> stream_;
  ProcState proc_state_;
  std::unique_ptr<DBM::Iterator> iter_;
  int32_t dbm_index_;
  tkrzw_rpc::IterateRequest request_;
  tkrzw_rpc::IterateResponse response_;
  grpc::Status rpc_status_;
};

class AsyncDBMProcessorReplicate : public AsyncDBMProcessorInterface {
 public:
  enum ProcState {CREATE, BEGIN, WRITING, WAITING, FINISH};

  AsyncDBMProcessorReplicate(
      DBMAsyncServiceImpl* service, grpc::ServerCompletionQueue* queue)
      : AsyncDBMProcessorInterface(&service->num_active_calls_),
        service_(service), queue_(queue),
        context_(), stream_(&context_), proc_state_(CREATE),
        reader_(nullptr), rpc_status_(grpc::Status::OK),
        deadline_(), alarm_(), wait_time_(0), bg_thread_(), alive_(true), mutex_() {
    Proceed();
  }

  ~AsyncDBMProcessorReplicate() {
    alive_.store(false);
    cond_.notify_one();
    if (bg_thread_.joinable()) {
      bg_thread_.join();
    }
  }

  void MonitorQueue() {
    while (alive_.load()) {
      if (proc_state_ == WAITING) {
        Status status(Status::SUCCESS);
        {
          std::lock_guard<std::mutex> lock(mutex_);
          status = reader_->Wait(1.0);
        }
        if (alive_.load() && status == Status::SUCCESS) {
          alarm_.Cancel();
        }
      }
      std::unique_lock<std::mutex> lock(mutex_);
      cond_.wait_for(lock, std::chrono::milliseconds(50));
    }
  }

  void Proceed() override {
    if (proc_state_ == CREATE) {
      context_.grpc::ServerContext::AsyncNotifyWhenDone(nullptr);
      proc_state_ = BEGIN;
      service_->RequestReplicate(&context_, &request_, &stream_, queue_, queue_, this);
    } else if (proc_state_ == BEGIN || proc_state_ == WRITING || proc_state_ == WAITING) {
      if (proc_state_ == BEGIN) {
        new AsyncDBMProcessorReplicate(service_, queue_);
        wait_time_ = request_.wait_time() < 0 ? INT32MAX : request_.wait_time();
        request_.set_wait_time(0);
      }
      response_.Clear();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        rpc_status_ = service_->ReplicateProcessOne(&reader_, &context_, request_, &response_);
      }
      if (proc_state_ == BEGIN) {
        bg_thread_ = std::thread([&]{ MonitorQueue(); });
      }
      if (rpc_status_.ok()) {
        if (response_.status().code() == Status::INFEASIBLE_ERROR &&
            proc_state_ == WRITING &&
            std::chrono::system_clock::now() <= deadline_) {
          proc_state_ = WAITING;
          alarm_.Set(queue_, deadline_, this);
          cond_.notify_one();
        } else {
          proc_state_ = WRITING;
          deadline_ = std::chrono::system_clock::now() + std::chrono::milliseconds(
              std::max<int64_t>(1, wait_time_ * 1000));
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

  void Cancel(bool is_shutdown) override {
    if (is_shutdown) {
      delete this;
    } else if (proc_state_ == WAITING) {
      Proceed();
    } else if (proc_state_ == WRITING) {
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
  grpc::ServerAsyncWriter<tkrzw_rpc::ReplicateResponse> stream_;
  std::atomic<ProcState> proc_state_;
  std::unique_ptr<MessageQueue::Reader> reader_;
  tkrzw_rpc::ReplicateRequest request_;
  tkrzw_rpc::ReplicateResponse response_;
  grpc::Status rpc_status_;
  std::chrono::time_point<std::chrono::system_clock> deadline_;
  grpc::Alarm alarm_;
  double wait_time_;
  std::thread bg_thread_;
  std::atomic_bool alive_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

inline void DBMAsyncServiceImpl::OperateQueue(
    grpc::ServerCompletionQueue* queue, const std::atomic_bool* is_shutdown) {
  logger_->Log(Logger::LEVEL_INFO, "Starting a completion queue");
  new AsyncDBMProcessor<tkrzw_rpc::EchoRequest, tkrzw_rpc::EchoResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestEcho,
      &DBMServiceBase::EchoImpl);
  new AsyncDBMProcessor<tkrzw_rpc::InspectRequest, tkrzw_rpc::InspectResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestInspect,
      &DBMServiceBase::InspectImpl);
  new AsyncDBMProcessor<tkrzw_rpc::GetRequest, tkrzw_rpc::GetResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGet,
      &DBMServiceBase::GetImpl);
  new AsyncDBMProcessor<tkrzw_rpc::GetMultiRequest, tkrzw_rpc::GetMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGetMulti,
      &DBMServiceBase::GetMultiImpl);
  new AsyncDBMProcessor<tkrzw_rpc::SetRequest, tkrzw_rpc::SetResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSet,
      &DBMServiceBase::SetImpl);
  new AsyncDBMProcessor<tkrzw_rpc::SetMultiRequest, tkrzw_rpc::SetMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSetMulti,
      &DBMServiceBase::SetMultiImpl);
  new AsyncDBMProcessor<tkrzw_rpc::RemoveRequest, tkrzw_rpc::RemoveResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRemove,
      &DBMServiceBase::RemoveImpl);
  new AsyncDBMProcessor<tkrzw_rpc::RemoveMultiRequest, tkrzw_rpc::RemoveMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRemoveMulti,
      &DBMServiceBase::RemoveMultiImpl);
  new AsyncDBMProcessor<tkrzw_rpc::AppendRequest, tkrzw_rpc::AppendResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestAppend,
      &DBMServiceBase::AppendImpl);
  new AsyncDBMProcessor<tkrzw_rpc::AppendMultiRequest, tkrzw_rpc::AppendMultiResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestAppendMulti,
      &DBMServiceBase::AppendMultiImpl);
  new AsyncDBMProcessorCompareExchange(this, queue);
  new AsyncDBMProcessor<tkrzw_rpc::IncrementRequest, tkrzw_rpc::IncrementResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestIncrement,
      &DBMServiceBase::IncrementImpl);
  new AsyncDBMProcessor<
    tkrzw_rpc::CompareExchangeMultiRequest, tkrzw_rpc::CompareExchangeMultiResponse>(
        this, queue, &DBMAsyncServiceImpl::RequestCompareExchangeMulti,
        &DBMServiceBase::CompareExchangeMultiImpl);
  new AsyncDBMProcessor<
    tkrzw_rpc::RekeyRequest, tkrzw_rpc::RekeyResponse>(
        this, queue, &DBMAsyncServiceImpl::RequestRekey,
        &DBMServiceBase::RekeyImpl);
  new AsyncDBMProcessorPopFirst(this, queue);
  new AsyncDBMProcessor<
    tkrzw_rpc::PushLastRequest, tkrzw_rpc::PushLastResponse>(
        this, queue, &DBMAsyncServiceImpl::RequestPushLast,
        &DBMServiceBase::PushLastAndNotify);
  new AsyncDBMProcessor<tkrzw_rpc::CountRequest, tkrzw_rpc::CountResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestCount,
      &DBMServiceBase::CountImpl);
  new AsyncDBMProcessor<tkrzw_rpc::GetFileSizeRequest, tkrzw_rpc::GetFileSizeResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestGetFileSize,
      &DBMServiceBase::GetFileSizeImpl);
  new AsyncDBMProcessor<tkrzw_rpc::ClearRequest, tkrzw_rpc::ClearResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestClear,
      &DBMServiceBase::ClearImpl);
  new AsyncDBMProcessorBackground<tkrzw_rpc::RebuildRequest, tkrzw_rpc::RebuildResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestRebuild,
      &DBMServiceBase::RebuildImpl);
  new AsyncDBMProcessor<tkrzw_rpc::ShouldBeRebuiltRequest, tkrzw_rpc::ShouldBeRebuiltResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestShouldBeRebuilt,
      &DBMServiceBase::ShouldBeRebuiltImpl);
  new AsyncDBMProcessorBackground<tkrzw_rpc::SynchronizeRequest, tkrzw_rpc::SynchronizeResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSynchronize,
      &DBMServiceBase::SynchronizeImpl);
  new AsyncDBMProcessor<tkrzw_rpc::SearchRequest, tkrzw_rpc::SearchResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestSearch,
      &DBMServiceBase::SearchImpl);
  new AsyncDBMProcessorStream(this, queue);
  new AsyncDBMProcessorIterate(this, queue);
  new AsyncDBMProcessorReplicate(this, queue);
  new AsyncDBMProcessor<tkrzw_rpc::ChangeMasterRequest, tkrzw_rpc::ChangeMasterResponse>(
      this, queue, &DBMAsyncServiceImpl::RequestChangeMaster,
      &DBMServiceBase::ChangeMasterImpl);
  num_standby_calls_ = num_active_calls_.load();
  while (true) {
    void* tag = nullptr;
    bool ok = false;
    if (!queue->Next(&tag, &ok)) {
      break;
    }
    auto* proc = static_cast<AsyncDBMProcessorInterface*>(tag);
    if (ok) {
      if (proc != nullptr) {
        proc->Proceed();
      }
    } else {
      if (proc != nullptr) {
        proc->Cancel(is_shutdown->load());
      }
      if (is_shutdown->load()) {
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
    auto* proc = static_cast<AsyncDBMProcessorInterface*>(tag);
    delete proc;
  }
}

}  // namespace tkrzw

#endif  // _TKRZW_SERVER_IMPL_H

// END OF FILE
