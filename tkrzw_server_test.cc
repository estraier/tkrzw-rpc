/*************************************************************************************************
 * Tests for tkrzw_server_impl.h
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

#include <google/protobuf/util/message_differencer.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "grpcpp/test/mock_stream.h"

#include "tkrzw_server_impl.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class ServerTest : public Test {};

// TODO: Remove this after prevailing packages supports the latest grpcpp/test/mock_stream.h
// where MockServerReaderWriter is implemented.
template <class W, class R>
class MockServerReaderWriter : public grpc::ServerReaderWriterInterface<W, R> {
 public:
  MockServerReaderWriter() = default;
  MOCK_METHOD0_T(SendInitialMetadata, void());
  MOCK_METHOD1_T(NextMessageSize, bool(uint32_t*));
  MOCK_METHOD1_T(Read, bool(R*));
  MOCK_METHOD2_T(Write, bool(const W&, const grpc::WriteOptions));
};

template <class W>
class MockServerWriter : public grpc::ServerWriterInterface<W> {
 public:
  MockServerWriter() = default;
  MOCK_METHOD0_T(SendInitialMetadata, void());
  MOCK_METHOD1_T(NextMessageSize, bool(uint32_t*));
  MOCK_METHOD2_T(Write, bool(const W&, const grpc::WriteOptions));
};

MATCHER_P(EqualsProto, rhs, "Equality matcher for protos") {
  return google::protobuf::util::MessageDifferencer::Equivalent(arg, rhs);
}

MATCHER_P(EqualsProtoStatus, rhs, "Equality matcher for protos") {
  return arg.status().code() == rhs.status().code();
}

TEST_F(ServerTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string hash_file_path = tmp_dir.MakeUniquePath();
  const std::string tree_file_path = tmp_dir.MakeUniquePath();
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(2);
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  dbms[1] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> hash_params =
      {{"dbm", "HashDBM"}, {"num_buckets", "20"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(hash_file_path, true, tkrzw::File::OPEN_DEFAULT, hash_params));
  const std::map<std::string, std::string> tree_params =
      {{"dbm", "TreeDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[1]->OpenAdvanced(tree_file_path, true, tkrzw::File::OPEN_DEFAULT, tree_params));
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger, 1, nullptr);
  grpc::ServerContext context;
  {
    tkrzw_rpc::EchoRequest request;
    request.set_message("hello");
    tkrzw_rpc::EchoResponse response;
    grpc::Status status = server.Echo(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("hello", response.echo());
  }
  {
    tkrzw_rpc::InspectRequest request;
    request.set_dbm_index(-1);
    tkrzw_rpc::InspectResponse response;
    grpc::Status status = server.Inspect(&context, &request, &response);
    std::map<std::string, std::string> records;
    for (const auto& record : response.records()) {
      records.emplace(std::make_pair(record.first(), record.second()));
    }
    EXPECT_EQ(_TKRPC_PKG_VERSION, records["version"]);
    EXPECT_EQ("2", records["num_dbms"]);
    EXPECT_EQ("0", records["dbm_0_count"]);
    EXPECT_EQ("0", records["dbm_1_count"]);
    EXPECT_EQ("HashDBM", records["dbm_0_class"]);
    EXPECT_EQ("TreeDBM", records["dbm_1_class"]);
  }
  {
    tkrzw_rpc::InspectRequest request;
    request.set_dbm_index(0);
    tkrzw_rpc::InspectResponse response;
    grpc::Status status = server.Inspect(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    std::map<std::string, std::string> records;
    for (const auto& record : response.records()) {
      records.emplace(std::make_pair(record.first(), record.second()));
    }
    EXPECT_EQ("HashDBM", records["class"]);
    EXPECT_EQ("20", records["num_buckets"]);
    EXPECT_EQ("0", records["num_records"]);
  }
  {
    tkrzw_rpc::InspectRequest request;
    request.set_dbm_index(1);
    tkrzw_rpc::InspectResponse response;
    grpc::Status status = server.Inspect(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    std::map<std::string, std::string> records;
    for (const auto& record : response.records()) {
      records.emplace(std::make_pair(record.first(), record.second()));
    }
    EXPECT_EQ("TreeDBM", records["class"]);
    EXPECT_EQ("1", records["tree_level"]);
    EXPECT_EQ("0", records["num_records"]);
  }
  {
    tkrzw_rpc::SetRequest request;
    request.set_key("one");
    request.set_value("first");
    tkrzw_rpc::SetResponse response;
    grpc::Status status = server.Set(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw_rpc::SetMultiRequest request;
    auto *record = request.add_records();
    record->set_first("two");
    record->set_second("second");
    record = request.add_records();
    record->set_first("three");
    record->set_second("third");
    tkrzw_rpc::SetMultiResponse response;
    grpc::Status status = server.SetMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw_rpc::AppendRequest request;
    request.set_key("one");
    request.set_value("1");
    request.set_delim(":");
    tkrzw_rpc::AppendResponse response;
    grpc::Status status = server.Append(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw_rpc::AppendMultiRequest request;
    auto *record = request.add_records();
    record->set_first("two");
    record->set_second("2");
    record = request.add_records();
    record->set_first("three");
    record->set_second("3");
    request.set_delim(":");
    tkrzw_rpc::AppendMultiResponse response;
    grpc::Status status = server.AppendMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw_rpc::CountRequest request;
    tkrzw_rpc::CountResponse response;
    grpc::Status status = server.Count(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(3, response.count());
  }
  {
    tkrzw_rpc::GetFileSizeRequest request;
    tkrzw_rpc::GetFileSizeResponse response;
    grpc::Status status = server.GetFileSize(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_GT(response.file_size(), 4096);
  }
  {
    tkrzw_rpc::GetRequest request;
    request.set_key("one");
    tkrzw_rpc::GetResponse response;
    grpc::Status status = server.Get(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("first:1", response.value());
  }
  {
    tkrzw_rpc::GetMultiRequest request;
    request.add_keys("two");
    request.add_keys("three");
    tkrzw_rpc::GetMultiResponse response;
    grpc::Status status = server.GetMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(2, response.records_size());
    std::map<std::string, std::string> records;
    for (const auto& record : response.records()) {
      records.emplace(record.first(), record.second());
    }
    EXPECT_EQ("second:2", records["two"]);
    EXPECT_EQ("third:3", records["three"]);
  }
  {
    tkrzw_rpc::RemoveRequest request;
    request.set_key("one");
    tkrzw_rpc::RemoveResponse response;
    grpc::Status status = server.Remove(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(2, dbms[0]->CountSimple());
  }
  {
    tkrzw_rpc::RemoveMultiRequest request;
    request.add_keys("two");
    request.add_keys("three");
    tkrzw_rpc::RemoveMultiResponse response;
    grpc::Status status = server.RemoveMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(0, dbms[0]->CountSimple());
  }
  {
    tkrzw_rpc::CompareExchangeRequest request;
    request.set_key("one");
    request.set_desired_existence(true);
    request.set_desired_value("ichi");
    tkrzw_rpc::CompareExchangeResponse response;
    grpc::Status status = server.CompareExchange(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_FALSE(response.found());
    EXPECT_EQ("ichi", dbms[0]->GetSimple("one"));
    request.Clear();
    request.set_key("one");
    request.set_expected_existence(true);
    request.set_expect_any_value(true);
    request.set_desire_no_update(true);
    request.set_get_actual(true);
    response.Clear();
    status = server.CompareExchange(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("ichi", response.actual());
    EXPECT_TRUE(response.found());
    request.Clear();
    request.set_key("one");
    request.set_expected_existence(true);
    request.set_expected_value("ichi");
    request.set_desired_existence(false);
    request.clear_desired_value();
    response.Clear();
    status = server.CompareExchange(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("*", dbms[0]->GetSimple("one", "*"));
  }
  {
    tkrzw_rpc::CompareExchangeMultiRequest request;
    auto* record = request.add_expected();
    record->set_key("two");
    record = request.add_expected();
    record->set_key("three");
    record = request.add_desired();
    record->set_existence(true);
    record->set_key("two");
    record->set_value("ni");
    record = request.add_desired();
    record->set_existence(true);
    record->set_key("three");
    record->set_value("san");
    tkrzw_rpc::CompareExchangeMultiResponse response;
    grpc::Status status = server.CompareExchangeMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("ni", dbms[0]->GetSimple("two", "*"));
    EXPECT_EQ("san", dbms[0]->GetSimple("three", "*"));
    request.Clear();
    record = request.add_expected();
    record->set_existence(true);
    record->set_key("two");
    record->set_value("ni");
    record = request.add_expected();
    record->set_existence(true);
    record->set_key("three");
    record->set_value("san");
    record = request.add_desired();
    record->set_key("two");
    record = request.add_desired();
    record->set_key("three");
    status = server.CompareExchangeMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(0, dbms[0]->CountSimple());
    request.Clear();
    record = request.add_expected();
    record->set_key("xyz");
    record = request.add_desired();
    record->set_existence(true);
    record->set_key("xyz");
    record->set_value("abc");
    response.Clear();
    server.CompareExchangeMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("abc", dbms[0]->GetSimple("xyz", "*"));
    request.Clear();
    record = request.add_expected();
    record->set_key("xyz");
    record->set_existence(true);
    record->set_any_value(true);
    record = request.add_desired();
    record->set_key("xyz");
    response.Clear();
    server.CompareExchangeMulti(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("*", dbms[0]->GetSimple("xyz", "*"));
  }
  {
    tkrzw_rpc::IncrementRequest request;
    request.set_key("num");
    request.set_increment(5);
    request.set_initial(100);
    tkrzw_rpc::IncrementResponse response;
    grpc::Status status = server.Increment(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(105, response.current());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Remove("num"));
  }
  {
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set("KEY1", "value1"));
    tkrzw_rpc::RekeyRequest request;
    request.set_old_key("KEY1");
    request.set_new_key("KEY2");
    request.set_overwrite(false);
    request.set_copying(true);
    tkrzw_rpc::RekeyResponse response;
    grpc::Status status = server.Rekey(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("value1", dbms[0]->GetSimple("KEY1", "*"));
    EXPECT_EQ("value1", dbms[0]->GetSimple("KEY2", "*"));
    request.set_overwrite(true);
    request.set_copying(false);
    status = server.Rekey(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("*", dbms[0]->GetSimple("KEY1", "*"));
    EXPECT_EQ("value1", dbms[0]->GetSimple("KEY2", "*"));
  }
  for (int32_t i = 0; i < 30; i++) {
    const std::string expr = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set(expr, expr));
  }
  {
    tkrzw_rpc::ShouldBeRebuiltRequest request;
    tkrzw_rpc::ShouldBeRebuiltResponse response;
    grpc::Status status = server.ShouldBeRebuilt(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_TRUE(response.tobe());
  }
  {
    tkrzw_rpc::RebuildRequest request;
    auto* param = request.add_params();
    param->set_first("align_pow");
    param->set_second("0");
    tkrzw_rpc::RebuildResponse response;
    grpc::Status status = server.Rebuild(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw_rpc::RebuildRequest request;
    auto* param = request.add_params();
    param->set_first("align_pow");
    param->set_second("0");
    tkrzw_rpc::RebuildResponse response;
    grpc::Status status = server.Rebuild(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    bool tobe = false;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->ShouldBeRebuilt(&tobe));
    EXPECT_FALSE(tobe);
  }
  {
    tkrzw_rpc::SynchronizeRequest request;
    tkrzw_rpc::SynchronizeResponse response;
    grpc::Status status = server.Synchronize(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set(key, ""));
    }
    tkrzw_rpc::SearchRequest request;
    request.set_mode("end");
    request.set_pattern("5");
    request.set_capacity(3);
    tkrzw_rpc::SearchResponse response;
    grpc::Status status = server.Search(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(3, response.matched_size());
    for (const auto& key : response.matched()) {
      EXPECT_TRUE(tkrzw::StrEndsWith(key, "5"));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[1]->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Close());
}

TEST_F(ServerTest, Stream) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(1);
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> params =
      {{"dbm", "HashDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(file_path, true, tkrzw::File::OPEN_DEFAULT, params));
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger, 1, nullptr);
  grpc::ServerContext context;
  MockServerReaderWriter<tkrzw_rpc::StreamResponse, tkrzw_rpc::StreamRequest> stream;
  tkrzw_rpc::StreamRequest request_echo;
  auto* echo_req = request_echo.mutable_echo_request();
  echo_req->set_message("hello");
  tkrzw_rpc::StreamRequest request_set;
  auto* set_req = request_set.mutable_set_request();
  set_req->set_key("key");
  set_req->set_value("value");
  tkrzw_rpc::StreamRequest request_append;
  auto* append_req = request_append.mutable_append_request();
  append_req->set_key("key");
  append_req->set_value("value");
  append_req->set_delim(":");
  tkrzw_rpc::StreamRequest request_get;
  auto* get_req = request_get.mutable_get_request();
  get_req->set_key("key");
  tkrzw_rpc::StreamRequest request_remove;
  auto* remove_req = request_remove.mutable_remove_request();
  remove_req->set_key("missing_key");
  tkrzw_rpc::StreamRequest request_compare_exchange;
  auto* compare_exchange_req = request_compare_exchange.mutable_compare_exchange_request();
  compare_exchange_req->set_key("key");
  tkrzw_rpc::StreamRequest request_increment;
  auto* increment_req = request_increment.mutable_increment_request();
  increment_req->set_key("num");
  increment_req->set_increment(5);
  increment_req->set_initial(100);
  tkrzw_rpc::StreamResponse response_echo;
  auto* echo_res = response_echo.mutable_echo_response();
  echo_res->set_echo("hello");
  tkrzw_rpc::StreamResponse response_set;
  response_set.mutable_set_response();
  tkrzw_rpc::StreamResponse response_get;
  auto* get_res = response_get.mutable_get_response();
  get_res->set_value("value:value");
  tkrzw_rpc::StreamResponse response_remove;
  auto* remove_res = response_remove.mutable_remove_response();
  remove_res->mutable_status()->set_code(tkrzw::Status::NOT_FOUND_ERROR);
  tkrzw_rpc::StreamResponse response_compare_exchange;
  auto* compare_exchange_res = response_compare_exchange.mutable_compare_exchange_response();
  compare_exchange_res->mutable_status()->set_code(tkrzw::Status::INFEASIBLE_ERROR);
  compare_exchange_res->set_found(true);
  tkrzw_rpc::StreamResponse response_increment;
  auto* increment_res = response_increment.mutable_increment_response();
  increment_res->set_current(105);
  EXPECT_CALL(stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(request_echo), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_set), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_append), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_remove), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_compare_exchange), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_increment), Return(true)))
      .WillOnce(Return(false));
  EXPECT_CALL(stream, Write(EqualsProto(response_echo), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_set), _)).WillRepeatedly(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_remove), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_compare_exchange), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_increment), _)).WillOnce(Return(true));
  grpc::Status status = server.StreamImpl(&context, &stream);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Close());
}

TEST_F(ServerTest, Iterator) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(1);
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> params =
      {{"dbm", "TreeDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(file_path, true, tkrzw::File::OPEN_DEFAULT, params));
  for (int32_t i = 1; i <= 10; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set(key, value));
  }
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger, 1, nullptr);
  grpc::ServerContext context;
  MockServerReaderWriter<tkrzw_rpc::IterateResponse, tkrzw_rpc::IterateRequest> stream;
  tkrzw_rpc::IterateRequest request_get;
  request_get.set_operation(tkrzw_rpc::IterateRequest::OP_GET);
  tkrzw_rpc::IterateRequest request_first;
  request_first.set_operation(tkrzw_rpc::IterateRequest::OP_FIRST);
  tkrzw_rpc::IterateRequest request_jump;
  request_jump.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP);
  request_jump.set_key("00000004");
  tkrzw_rpc::IterateRequest request_next;
  request_next.set_operation(tkrzw_rpc::IterateRequest::OP_NEXT);
  tkrzw_rpc::IterateRequest request_jump_upper;
  request_jump_upper.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP_UPPER);
  request_jump_upper.set_key("00000008");
  request_jump_upper.set_jump_inclusive(true);
  tkrzw_rpc::IterateRequest request_previous;
  request_previous.set_operation(tkrzw_rpc::IterateRequest::OP_PREVIOUS);
  tkrzw_rpc::IterateRequest request_last;
  request_last.set_operation(tkrzw_rpc::IterateRequest::OP_LAST);
  tkrzw_rpc::IterateRequest request_set;
  request_set.set_operation(tkrzw_rpc::IterateRequest::OP_SET);
  request_set.set_value("setvalue");
  tkrzw_rpc::IterateRequest request_remove;
  request_remove.set_operation(tkrzw_rpc::IterateRequest::OP_REMOVE);
  tkrzw_rpc::IterateResponse response_move;
  tkrzw_rpc::IterateResponse response_get_first;
  response_get_first.set_key("00000001");
  response_get_first.set_value("1");
  tkrzw_rpc::IterateResponse response_get_jump;
  response_get_jump.set_key("00000004");
  response_get_jump.set_value("16");
  tkrzw_rpc::IterateResponse response_get_next;
  response_get_next.set_key("00000005");
  response_get_next.set_value("25");
  tkrzw_rpc::IterateResponse response_get_jump_upper;
  response_get_jump_upper.set_key("00000008");
  response_get_jump_upper.set_value("64");
  tkrzw_rpc::IterateResponse response_get_previous;
  response_get_previous.set_key("00000007");
  response_get_previous.set_value("49");
  tkrzw_rpc::IterateResponse response_get_last;
  response_get_last.set_key("00000010");
  response_get_last.set_value("100");
  tkrzw_rpc::IterateResponse response_get_set;
  response_get_set.set_key("00000010");
  response_get_set.set_value("setvalue");
  tkrzw_rpc::IterateResponse response_get_remove;
  response_get_remove.mutable_status()->set_code(tkrzw::Status::NOT_FOUND_ERROR);
  EXPECT_CALL(stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(request_first), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_jump), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_next), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_jump_upper), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_previous), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_last), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_set), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_remove), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_get), Return(true)))
      .WillOnce(Return(false));
  EXPECT_CALL(stream, Write(EqualsProto(response_move), _)).WillRepeatedly(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_first), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_jump), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_next), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_jump_upper), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_previous), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_last), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_set), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_get_remove), _)).WillOnce(Return(true));
  grpc::Status status = server.IterateImpl(&context, &stream);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Close());
}

TEST_F(ServerTest, Queue) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(1);
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> params =
      {{"dbm", "TreeDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(file_path, true, tkrzw::File::OPEN_DEFAULT, params));
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger, 1, nullptr);
  grpc::ServerContext context;
  for (int32_t i = 0; i < 3; i++) {
    tkrzw_rpc::PushLastRequest request;
    request.set_value(tkrzw::ToString(i));
    tkrzw_rpc::PushLastResponse response;
    grpc::Status status = server.PushLast(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  EXPECT_EQ(3, dbms[0]->CountSimple());
  MockServerReaderWriter<tkrzw_rpc::IterateResponse, tkrzw_rpc::IterateRequest> stream;
  tkrzw_rpc::IterateRequest request_first;
  request_first.set_operation(tkrzw_rpc::IterateRequest::OP_FIRST);
  tkrzw_rpc::IterateRequest request_step;
  request_step.set_operation(tkrzw_rpc::IterateRequest::OP_STEP);
  tkrzw_rpc::IterateResponse response_first;
  tkrzw_rpc::IterateResponse response_step1;
  response_step1.set_key(std::string("\0\0\0\0\0\0\0\0", 8));
  response_step1.set_value("0");
  tkrzw_rpc::IterateResponse response_step2;
  response_step2.set_key(std::string("\0\0\0\0\0\0\0\1", 8));
  response_step2.set_value("1");
  tkrzw_rpc::IterateResponse response_step3;
  response_step3.set_key(std::string("\0\0\0\0\0\0\0\2", 8));
  response_step3.set_value("2");
  EXPECT_CALL(stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(request_first), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_step), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_step), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(request_step), Return(true)))
      .WillOnce(Return(false));
  EXPECT_CALL(stream, Write(EqualsProto(response_first), _)).WillRepeatedly(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_step1), _)).WillRepeatedly(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_step2), _)).WillRepeatedly(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_step3), _)).WillRepeatedly(Return(true));
  grpc::Status status = server.IterateImpl(&context, &stream);
  EXPECT_TRUE(status.ok());
  for (int32_t i = 0; i < 3; i++) {
    tkrzw_rpc::PopFirstRequest request;
    tkrzw_rpc::PopFirstResponse response;
    grpc::Status status = server.PopFirst(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(std::string("\0\0\0\0\0\0\0", 7) + char(i), response.key());
    EXPECT_EQ(tkrzw::ToString(i), response.value());
  }
  EXPECT_EQ(0, dbms[0]->CountSimple());
  tkrzw_rpc::PopFirstRequest request;
  tkrzw_rpc::PopFirstResponse response;
  status = server.PopFirst(&context, &request, &response);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, response.status().code());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Close());
}

TEST_F(ServerTest, Replicator) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string ulog_prefix = file_path + "-ulog";
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(1);
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> params =
      {{"dbm", "TreeDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(file_path, true, tkrzw::File::OPEN_DEFAULT, params));
  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(ulog_prefix, 50));
  tkrzw::DBMUpdateLoggerMQ ulog(&mq, 123, 321, 100);
  dbms[0]->SetUpdateLogger(&ulog);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Clear());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set("key", "value"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Append("key", "value", ":"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Remove("key"));
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger, 2, &mq);
  grpc::ServerContext context;
  MockServerWriter<tkrzw_rpc::ReplicateResponse> stream;
  tkrzw_rpc::ReplicateRequest request_start;
  request_start.set_min_timestamp(100);
  request_start.set_server_id(1);
  request_start.set_wait_time(0);
  tkrzw_rpc::ReplicateResponse response_start;
  response_start.set_server_id(2);
  tkrzw_rpc::ReplicateResponse response_write1;
  response_write1.set_timestamp(100);
  response_write1.set_server_id(123);
  response_write1.set_dbm_index(321);
  response_write1.set_op_type(tkrzw_rpc::ReplicateResponse::OP_CLEAR);
  tkrzw_rpc::ReplicateResponse response_write2;
  response_write2.set_timestamp(100);
  response_write2.set_server_id(123);
  response_write2.set_dbm_index(321);
  response_write2.set_op_type(tkrzw_rpc::ReplicateResponse::OP_SET);
  response_write2.set_key("key");
  response_write2.set_value("value");
  tkrzw_rpc::ReplicateResponse response_write3;
  response_write3.set_timestamp(100);
  response_write3.set_server_id(123);
  response_write3.set_dbm_index(321);
  response_write3.set_op_type(tkrzw_rpc::ReplicateResponse::OP_SET);
  response_write3.set_key("key");
  response_write3.set_value("value:value");
  tkrzw_rpc::ReplicateResponse response_write4;
  response_write4.set_timestamp(100);
  response_write4.set_server_id(123);
  response_write4.set_dbm_index(321);
  response_write4.set_op_type(tkrzw_rpc::ReplicateResponse::OP_REMOVE);
  response_write4.set_key("key");
  tkrzw_rpc::ReplicateResponse response_end;
  response_end.mutable_status()->set_code(tkrzw::Status::INFEASIBLE_ERROR);
  EXPECT_CALL(stream, Write(EqualsProto(response_start), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_write1), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_write2), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_write3), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProto(response_write4), _)).WillOnce(Return(true));
  EXPECT_CALL(stream, Write(EqualsProtoStatus(response_end), _))
      .WillOnce(Return(true)).WillOnce(Return(false));
  grpc::Status status = server.ReplicateImpl(&context, &request_start, &stream);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Close());
}

// END OF FILE
