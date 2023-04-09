/*************************************************************************************************
 * Tests for tkrzw_dbm_remote.h
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

#include "tkrzw_dbm_remote.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_rpc_mock.grpc.pb.h"
#include "tkrzw_rpc.pb.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class RemoteDBMTest : public Test {};

MATCHER_P(EqualsProto, rhs, "Equality matcher for protos") {
  return google::protobuf::util::MessageDifferencer::Equivalent(arg, rhs);
}

TEST_F(RemoteDBMTest, Echo) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::EchoRequest request;
  request.set_message("hello");
  tkrzw_rpc::EchoResponse response;
  response.set_echo("hello");
  EXPECT_CALL(*stub, Echo(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  std::string echo;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Echo("hello", &echo));
  EXPECT_EQ("hello", echo);
}

TEST_F(RemoteDBMTest, Inspect) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::InspectRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::InspectResponse response;
  auto* res_record = response.add_records();
  res_record->set_first("name");
  res_record->set_second("value");
  EXPECT_CALL(*stub, Inspect(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::vector<std::pair<std::string, std::string>> records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Inspect(&records));
  ASSERT_EQ(1, records.size());
  EXPECT_EQ("name", records[0].first);
  EXPECT_EQ("value", records[0].second);
}

TEST_F(RemoteDBMTest, InspectServer) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::InspectRequest request;
  request.set_dbm_index(-1);
  tkrzw_rpc::InspectResponse response;
  auto* res_record = response.add_records();
  res_record->set_first("name");
  res_record->set_second("value");
  EXPECT_CALL(*stub, Inspect(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(-1);
  std::vector<std::pair<std::string, std::string>> records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Inspect(&records));
  ASSERT_EQ(1, records.size());
  EXPECT_EQ("name", records[0].first);
  EXPECT_EQ("value", records[0].second);
}

TEST_F(RemoteDBMTest, Get) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::GetRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  tkrzw_rpc::GetResponse response;
  response.set_value("value");
  EXPECT_CALL(*stub, Get(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get("key", &value));
  EXPECT_EQ("value", value);
}

TEST_F(RemoteDBMTest, GetMulti) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::GetMultiRequest request;
  request.set_dbm_index(123);
  request.add_keys("key");
  tkrzw_rpc::GetMultiResponse response;
  auto* record = response.add_records();
  record->set_first("key");
  record->set_second("value");
  EXPECT_CALL(*stub, GetMulti(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::map<std::string, std::string> records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.GetMulti({"key"}, &records));
  EXPECT_EQ(1, records.size());
  EXPECT_EQ("value", records["key"]);
}

TEST_F(RemoteDBMTest, Set) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::SetRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  request.set_value("value");
  request.set_overwrite(true);
  tkrzw_rpc::SetResponse response;
  EXPECT_CALL(*stub, Set(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("key", "value"));
}

TEST_F(RemoteDBMTest, SetMulti) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::SetMultiRequest request;
  request.set_dbm_index(123);
  auto* record = request.add_records();
  record->set_first("key");
  record->set_second("value");
  request.set_overwrite(true);
  tkrzw_rpc::SetMultiResponse response;
  EXPECT_CALL(*stub, SetMulti(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.SetMulti({{"key", "value"}}));
}

TEST_F(RemoteDBMTest, Remove) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::RemoveRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  tkrzw_rpc::RemoveResponse response;
  EXPECT_CALL(*stub, Remove(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove("key"));
}

TEST_F(RemoteDBMTest, RemoveMulti) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::RemoveMultiRequest request;
  request.set_dbm_index(123);
  request.add_keys("key");
  tkrzw_rpc::RemoveMultiResponse response;
  EXPECT_CALL(*stub, RemoveMulti(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RemoveMulti({"key"}));
}

TEST_F(RemoteDBMTest, Append) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::AppendRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  request.set_value("value");
  request.set_delim(":");
  tkrzw_rpc::AppendResponse response;
  EXPECT_CALL(*stub, Append(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Append("key", "value", ":"));
}

TEST_F(RemoteDBMTest, AppendMulti) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::AppendMultiRequest request;
  request.set_dbm_index(123);
  auto* record = request.add_records();
  record->set_first("key");
  record->set_second("value");
  request.set_delim(":");
  tkrzw_rpc::AppendMultiResponse response;
  EXPECT_CALL(*stub, AppendMulti(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.AppendMulti({{"key", "value"}}, ":"));
}

TEST_F(RemoteDBMTest, CompareExchange) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::CompareExchangeRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  request.set_expected_existence(true);
  request.set_expected_value("expected");
  request.set_desired_existence(true);
  request.set_desired_value("desired");
  tkrzw_rpc::CompareExchangeResponse response;
  response.set_found(true);
  EXPECT_CALL(*stub, CompareExchange(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.CompareExchange("key", "expected", "desired"));
}

TEST_F(RemoteDBMTest, CompareExchangeAdvanced) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::CompareExchangeRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  request.set_expected_existence(true);
  request.set_expect_any_value(true);
  request.set_desire_no_update(true);
  request.set_get_actual(true);
  request.set_retry_wait(10);
  request.set_notify(true);
  tkrzw_rpc::CompareExchangeResponse response;
  response.set_actual("actual");
  response.set_found(true);
  EXPECT_CALL(*stub, CompareExchange(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string actual;
  bool found = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.CompareExchange(
      "key", tkrzw::DBM::ANY_DATA, tkrzw::DBM::ANY_DATA, &actual, &found, 10, true));
  EXPECT_TRUE(found);
  EXPECT_EQ("actual", actual);
}

TEST_F(RemoteDBMTest, Increment) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::IncrementRequest request;
  request.set_dbm_index(123);
  request.set_key("key");
  request.set_increment(5);
  request.set_initial(100);
  tkrzw_rpc::IncrementResponse response;
  response.set_current(105);
  EXPECT_CALL(*stub, Increment(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  int64_t current = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Increment("key", 5, &current, 100));
  EXPECT_EQ(105, current);
}

TEST_F(RemoteDBMTest, CompareExchangeMulti) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::CompareExchangeMultiRequest request;
  request.set_dbm_index(123);
  auto* req_expected = request.add_expected();
  req_expected->set_existence(true);
  req_expected->set_key("expected_key");
  req_expected->set_value("expected_value");
  auto* req_desired = request.add_desired();
  req_desired->set_existence(true);
  req_desired->set_key("desired_key");
  req_desired->set_value("desired_value");
  tkrzw_rpc::CompareExchangeMultiResponse response;
  EXPECT_CALL(*stub, CompareExchangeMulti(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::vector<std::pair<std::string_view, std::string_view>> expected;
  expected.emplace_back(std::make_pair(
      std::string_view("expected_key"), std::string_view("expected_value")));
  std::vector<std::pair<std::string_view, std::string_view>> desired;
  desired.emplace_back(std::make_pair(
      std::string_view("desired_key"), std::string_view("desired_value")));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.CompareExchangeMulti(expected, desired));
}

TEST_F(RemoteDBMTest, Rekey) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::RekeyRequest request;
  request.set_dbm_index(123);
  request.set_old_key("old_key");
  request.set_new_key("new_key");
  request.set_overwrite(true);
  request.set_copying(true);
  tkrzw_rpc::RekeyResponse response;
  EXPECT_CALL(*stub, Rekey(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Rekey("old_key", "new_key", true, true));
}

TEST_F(RemoteDBMTest, PopFirst) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::PopFirstRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::PopFirstResponse response;
  response.set_key("key");
  response.set_value("value");
  EXPECT_CALL(*stub, PopFirst(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string key, value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.PopFirst(&key, &value));
  EXPECT_EQ("key", key);
  EXPECT_EQ("value", value);
}

TEST_F(RemoteDBMTest, PushLast) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::PushLastRequest request;
  request.set_dbm_index(123);
  request.set_value("value");
  request.set_wtime(10);
  tkrzw_rpc::PushLastResponse response;
  EXPECT_CALL(*stub, PushLast(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.PushLast("value", 10));
}

TEST_F(RemoteDBMTest, Count) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::CountRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::CountResponse response;
  response.set_count(123);
  EXPECT_CALL(*stub, Count(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  int64_t count = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Count(&count));
  EXPECT_EQ(123, count);
}

TEST_F(RemoteDBMTest, GetFileSize) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::GetFileSizeRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::GetFileSizeResponse response;
  response.set_file_size(1234);
  EXPECT_CALL(*stub, GetFileSize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  int64_t file_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.GetFileSize(&file_size));
  EXPECT_EQ(1234, file_size);
}

TEST_F(RemoteDBMTest, Clear) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::ClearRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::ClearResponse response;
  EXPECT_CALL(*stub, Clear(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
}

TEST_F(RemoteDBMTest, Rebuild) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::RebuildRequest request;
  request.set_dbm_index(123);
  auto* param = request.add_params();
  param->set_first("num_buckets");
  param->set_second("10");
  tkrzw_rpc::RebuildResponse response;
  EXPECT_CALL(*stub, Rebuild(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Rebuild({{"num_buckets", "10"}}));
}

TEST_F(RemoteDBMTest, ShouldBeRebuilt) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::ShouldBeRebuiltRequest request;
  request.set_dbm_index(123);
  tkrzw_rpc::ShouldBeRebuiltResponse response;
  response.set_tobe(true);
  EXPECT_CALL(*stub, ShouldBeRebuilt(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  bool tobe = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.ShouldBeRebuilt(&tobe));
  EXPECT_TRUE(tobe);
}

TEST_F(RemoteDBMTest, Synchronize) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::SynchronizeRequest request;
  request.set_dbm_index(123);
  request.set_hard(true);
  auto* param = request.add_params();
  param->set_first("reducer");
  param->set_second("last");
  tkrzw_rpc::SynchronizeResponse response;
  EXPECT_CALL(*stub, Synchronize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Synchronize(true, {{"reducer", "last"}}));
}

TEST_F(RemoteDBMTest, Search) {
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  tkrzw_rpc::SearchRequest request;
  request.set_dbm_index(123);
  request.set_mode("end");
  request.set_pattern("5");
  request.set_capacity(3);
  tkrzw_rpc::SearchResponse response;
  response.add_matched("5");
  response.add_matched("15");
  response.add_matched("25");
  EXPECT_CALL(*stub, Search(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  std::vector<std::string> matched;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Search("end", "5", &matched, 3));
  EXPECT_THAT(matched, UnorderedElementsAre("5", "15", "25"));
}

TEST_F(RemoteDBMTest, Stream) {
  auto stream = std::make_unique<grpc::testing::MockClientReaderWriter<
    tkrzw_rpc::StreamRequest, tkrzw_rpc::StreamResponse>>();
  tkrzw_rpc::StreamRequest request_set;
  auto* set_req = request_set.mutable_set_request();
  set_req->set_dbm_index(123);
  set_req->set_key("key");
  set_req->set_value("value");
  set_req->set_overwrite(true);
  tkrzw_rpc::StreamRequest request_get;
  auto* get_req = request_get.mutable_get_request();
  get_req->set_dbm_index(123);
  get_req->set_key("key");
  tkrzw_rpc::StreamRequest request_remove;
  auto* remove_req = request_remove.mutable_remove_request();
  remove_req->set_dbm_index(123);
  remove_req->set_key("missing_key");
  tkrzw_rpc::StreamRequest request_append;
  auto* append_req = request_append.mutable_append_request();
  append_req->set_dbm_index(123);
  append_req->set_key("key");
  append_req->set_value("value");
  append_req->set_delim(":");
  tkrzw_rpc::StreamRequest request_compare_exchange;
  auto* compare_exchange_req = request_compare_exchange.mutable_compare_exchange_request();
  compare_exchange_req->set_dbm_index(123);
  compare_exchange_req->set_key("key");
  compare_exchange_req->set_expected_existence(true);
  compare_exchange_req->set_expected_value("expected");
  compare_exchange_req->set_desired_existence(true);
  compare_exchange_req->set_desired_value("desired");
  tkrzw_rpc::StreamRequest request_increment;
  auto* increment_req = request_increment.mutable_increment_request();
  increment_req->set_dbm_index(123);
  increment_req->set_key("key");
  increment_req->set_increment(5);
  increment_req->set_initial(100);
  tkrzw_rpc::StreamResponse response_set;
  response_set.mutable_set_response();
  tkrzw_rpc::StreamResponse response_get;
  auto* get_res = response_get.mutable_get_response();
  get_res->set_value("value");
  tkrzw_rpc::StreamResponse response_remove;
  auto* remove_res = response_remove.mutable_remove_response();
  remove_res->mutable_status()->set_code(tkrzw::Status::NOT_FOUND_ERROR);
  tkrzw_rpc::StreamResponse response_append;
  response_append.mutable_append_response();
  tkrzw_rpc::StreamResponse response_compare_exchange;
  response_compare_exchange.mutable_compare_exchange_response();
  tkrzw_rpc::StreamResponse response_increment;
  auto* increment_res = response_increment.mutable_increment_response();
  increment_res->set_current(105);
  EXPECT_CALL(*stream, Write(EqualsProto(request_set), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_get), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_remove), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_append), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_compare_exchange), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_increment), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(response_set), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_remove), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_append), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_compare_exchange), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_increment), Return(true)));
  EXPECT_CALL(*stream, WritesDone()).WillOnce(Return(true));
  EXPECT_CALL(*stream, Finish()).WillOnce(Return(grpc::Status::OK));
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  EXPECT_CALL(*stub, StreamRaw(_)).WillRepeatedly(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  auto strm = dbm.MakeStream();
  EXPECT_EQ(tkrzw::Status::SUCCESS, strm->Set("key", "value"));
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, strm->Get("key", &value));
  EXPECT_EQ("value", value);
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, strm->Remove("missing_key"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, strm->Append("key", "value", ":"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, strm->CompareExchange("key", "expected", "desired"));
  int64_t current = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, strm->Increment("key", 5, &current, 100));
  EXPECT_EQ(105, current);
}

TEST_F(RemoteDBMTest, IterateMove) {
  auto stream = std::make_unique<grpc::testing::MockClientReaderWriter<
    tkrzw_rpc::IterateRequest, tkrzw_rpc::IterateResponse>>();
  tkrzw_rpc::IterateRequest request_first;
  request_first.set_dbm_index(123);
  request_first.set_operation(tkrzw_rpc::IterateRequest::OP_FIRST);
  tkrzw_rpc::IterateRequest request_last;
  request_last.set_dbm_index(123);
  request_last.set_operation(tkrzw_rpc::IterateRequest::OP_LAST);
  tkrzw_rpc::IterateRequest request_jump;
  request_jump.set_dbm_index(123);
  request_jump.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP);
  request_jump.set_key("jump");
  tkrzw_rpc::IterateRequest request_jump_lower;
  request_jump_lower.set_dbm_index(123);
  request_jump_lower.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP_LOWER);
  request_jump_lower.set_key("jumplower");
  tkrzw_rpc::IterateRequest request_jump_lower_inc;
  request_jump_lower_inc.set_dbm_index(123);
  request_jump_lower_inc.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP_LOWER);
  request_jump_lower_inc.set_key("jumplowerinc");
  request_jump_lower_inc.set_jump_inclusive(true);
  tkrzw_rpc::IterateRequest request_jump_upper;
  request_jump_upper.set_dbm_index(123);
  request_jump_upper.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP_UPPER);
  request_jump_upper.set_key("jumpupper");
  tkrzw_rpc::IterateRequest request_jump_upper_inc;
  request_jump_upper_inc.set_dbm_index(123);
  request_jump_upper_inc.set_operation(tkrzw_rpc::IterateRequest::OP_JUMP_UPPER);
  request_jump_upper_inc.set_key("jumpupperinc");
  request_jump_upper_inc.set_jump_inclusive(true);
  tkrzw_rpc::IterateRequest request_next;
  request_next.set_dbm_index(123);
  request_next.set_operation(tkrzw_rpc::IterateRequest::OP_NEXT);
  tkrzw_rpc::IterateRequest request_previous;
  request_previous.set_dbm_index(123);
  request_previous.set_operation(tkrzw_rpc::IterateRequest::OP_PREVIOUS);
  tkrzw_rpc::IterateResponse response;
  EXPECT_CALL(*stream, Write(EqualsProto(request_first), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_last), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_jump), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_jump_lower), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_jump_lower_inc), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_jump_upper), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_jump_upper_inc), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_next), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_previous), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Read(_)).WillRepeatedly(DoAll(SetArgPointee<0>(response), Return(true)));
  EXPECT_CALL(*stream, WritesDone()).WillOnce(Return(true));
  EXPECT_CALL(*stream, Finish()).WillOnce(Return(grpc::Status::OK));
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  EXPECT_CALL(*stub, IterateRaw(_)).WillRepeatedly(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);
  auto iter = dbm.MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Last());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("jump"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("jumplower"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("jumplowerinc", true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("jumpupper"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("jumpupperinc", true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
}

TEST_F(RemoteDBMTest, IterateAction) {
  auto stream = std::make_unique<grpc::testing::MockClientReaderWriter<
    tkrzw_rpc::IterateRequest, tkrzw_rpc::IterateResponse>>();
  tkrzw_rpc::IterateRequest request_get_both;
  request_get_both.set_dbm_index(123);
  request_get_both.set_operation(tkrzw_rpc::IterateRequest::OP_GET);
  tkrzw_rpc::IterateRequest request_get_none;
  request_get_none.set_dbm_index(123);
  request_get_none.set_operation(tkrzw_rpc::IterateRequest::OP_GET);
  request_get_none.set_omit_key(true);
  request_get_none.set_omit_value(true);
  tkrzw_rpc::IterateRequest request_get_key;
  request_get_key.set_dbm_index(123);
  request_get_key.set_operation(tkrzw_rpc::IterateRequest::OP_GET);
  request_get_key.set_omit_value(true);
  tkrzw_rpc::IterateRequest request_get_value;
  request_get_value.set_dbm_index(123);
  request_get_value.set_operation(tkrzw_rpc::IterateRequest::OP_GET);
  request_get_value.set_omit_key(true);
  tkrzw_rpc::IterateRequest request_set;
  request_set.set_dbm_index(123);
  request_set.set_operation(tkrzw_rpc::IterateRequest::OP_SET);
  request_set.set_value("set");
  tkrzw_rpc::IterateRequest request_remove;
  request_remove.set_dbm_index(123);
  request_remove.set_operation(tkrzw_rpc::IterateRequest::OP_REMOVE);
  tkrzw_rpc::IterateResponse response_get_both;
  response_get_both.set_key("getbothkey");
  response_get_both.set_value("getbothvalue");
  tkrzw_rpc::IterateResponse response_get_none;
  response_get_none.mutable_status()->set_code(tkrzw::Status::NOT_FOUND_ERROR);
  tkrzw_rpc::IterateResponse response_get_key;
  response_get_key.set_key("getkeykey");
  tkrzw_rpc::IterateResponse response_get_value;
  response_get_value.set_value("getvaluevalue");
  tkrzw_rpc::IterateResponse response;
  EXPECT_CALL(*stream, Write(EqualsProto(request_get_both), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_get_none), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_get_key), _))
      .WillOnce(Return(true)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_get_value), _))
      .WillOnce(Return(true)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_set), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Write(EqualsProto(request_remove), _)).WillOnce(Return(true));
  EXPECT_CALL(*stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_both), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_none), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_key), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_none), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_value), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_get_none), Return(true)))
      .WillRepeatedly(DoAll(SetArgPointee<0>(response), Return(true)));
  EXPECT_CALL(*stream, WritesDone()).WillOnce(Return(true));
  EXPECT_CALL(*stream, Finish()).WillOnce(Return(grpc::Status::OK));
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  EXPECT_CALL(*stub, IterateRaw(_)).WillRepeatedly(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  dbm.SetDBMIndex(123);  
  auto iter = dbm.MakeIterator();
  std::string key, value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&key, &value));
  EXPECT_EQ("getbothkey", key);
  EXPECT_EQ("getbothvalue", value);
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  EXPECT_EQ("getkeykey", iter->GetKey());
  EXPECT_EQ("*", iter->GetKey("*"));
  EXPECT_EQ("getvaluevalue", iter->GetValue());
  EXPECT_EQ("*", iter->GetValue("*"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Set("set"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Remove());
}

TEST_F(RemoteDBMTest, Replicate) {
  auto stream = std::make_unique<grpc::testing::MockClientReader<tkrzw_rpc::ReplicateResponse>>();
  tkrzw_rpc::ReplicateRequest request;
  request.set_min_timestamp(100);
  request.set_server_id(123);
  request.set_wait_time(2.0);
  tkrzw_rpc::ReplicateResponse response_start;
  response_start.set_server_id(2);
  tkrzw_rpc::ReplicateResponse response_read1;
  response_read1.set_timestamp(110);
  response_read1.set_server_id(2);
  response_read1.set_dbm_index(3);
  response_read1.set_op_type(tkrzw_rpc::ReplicateResponse::OP_SET);
  response_read1.set_key("key1");
  response_read1.set_value("value1");
  tkrzw_rpc::ReplicateResponse response_read2;
  response_read2.set_timestamp(120);
  response_read2.set_server_id(4);
  response_read2.set_dbm_index(6);
  response_read2.set_op_type(tkrzw_rpc::ReplicateResponse::OP_REMOVE);
  response_read2.set_key("key2");
  tkrzw_rpc::ReplicateResponse response_read3;
  response_read3.set_timestamp(130);
  response_read3.set_server_id(6);
  response_read3.set_dbm_index(9);
  response_read3.set_op_type(tkrzw_rpc::ReplicateResponse::OP_CLEAR);
  tkrzw_rpc::ReplicateResponse response_read4;
  response_read4.mutable_status()->set_code(tkrzw::Status::INFEASIBLE_ERROR);
  response_read4.set_timestamp(140);
  EXPECT_CALL(*stream, Read(_))
      .WillOnce(DoAll(SetArgPointee<0>(response_start), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_read1), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_read2), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_read3), Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>(response_read4), Return(true)));
  auto stub = std::make_unique<tkrzw_rpc::MockDBMServiceStub>();
  EXPECT_CALL(*stub, ReplicateRaw(_, EqualsProto(request)))
      .WillOnce(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  auto repl = dbm.MakeReplicator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, repl->Start(100, 123, 2.0));
  EXPECT_EQ(2, repl->GetMasterServerID());
  int64_t timestamp = 0;
  tkrzw::RemoteDBM::ReplicateLog op;
  EXPECT_EQ(tkrzw::Status::SUCCESS, repl->Read(&timestamp, &op));
  EXPECT_EQ(110, timestamp);
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_SET, op.op_type);
  EXPECT_EQ(2, op.server_id);
  EXPECT_EQ(3, op.dbm_index);
  EXPECT_EQ("key1", op.key);
  EXPECT_EQ("value1", op.value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, repl->Read(&timestamp, &op));
  EXPECT_EQ(120, timestamp);
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_REMOVE, op.op_type);
  EXPECT_EQ(4, op.server_id);
  EXPECT_EQ(6, op.dbm_index);
  EXPECT_EQ("key2", op.key);
  EXPECT_EQ(tkrzw::Status::SUCCESS, repl->Read(&timestamp, &op));
  EXPECT_EQ(130, timestamp);
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_CLEAR, op.op_type);
  EXPECT_EQ(6, op.server_id);
  EXPECT_EQ(9, op.dbm_index);
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, repl->Read(&timestamp, &op));
  EXPECT_EQ(140, timestamp);
}

// END OF FILE
