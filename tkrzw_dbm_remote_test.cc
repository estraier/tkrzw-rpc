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

#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

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

class ClientTest : public Test {};

MATCHER_P(EqualsProto, rhs, "Equality matcher for protos") {
  return google::protobuf::util::MessageDifferencer::Equivalent(arg, rhs);
}

TEST_F(ClientTest, Echo) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::EchoRequest request;
  request.set_message("hello");
  tkrzw::EchoResponse response;
  response.set_echo("hello");
  EXPECT_CALL(*stub, Echo(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  std::string echo;
  const tkrzw::Status status = client.Echo("hello", &echo);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ("hello", echo);
}

TEST_F(ClientTest, Inspect) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::InspectRequest request;
  tkrzw::InspectResponse response;
  auto* res_record = response.add_records();
  res_record->set_first("name");
  res_record->set_second("value");
  EXPECT_CALL(*stub, Inspect(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  std::vector<std::pair<std::string, std::string>> records;
  const tkrzw::Status status = client.Inspect(&records);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  ASSERT_EQ(1, records.size());
  EXPECT_EQ("name", records[0].first);
  EXPECT_EQ("value", records[0].second);
}

TEST_F(ClientTest, InspectServer) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::InspectRequest request;
  request.set_dbm_index(-1);
  tkrzw::InspectResponse response;
  auto* res_record = response.add_records();
  res_record->set_first("name");
  res_record->set_second("value");
  EXPECT_CALL(*stub, Inspect(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  client.SetDBMIndex(-1);
  std::vector<std::pair<std::string, std::string>> records;
  const tkrzw::Status status = client.Inspect(&records);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  ASSERT_EQ(1, records.size());
  EXPECT_EQ("name", records[0].first);
  EXPECT_EQ("value", records[0].second);
}

TEST_F(ClientTest, Get) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::GetRequest request;
  request.set_key("key");
  tkrzw::GetResponse response;
  response.set_value("value");
  EXPECT_CALL(*stub, Get(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  std::string value;
  const tkrzw::Status status = client.Get("key", &value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ("value", value);
}

TEST_F(ClientTest, Set) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::SetRequest request;
  request.set_key("key");
  request.set_value("value");
  request.set_overwrite(true);
  tkrzw::SetResponse response;
  EXPECT_CALL(*stub, Set(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  std::string value;
  const tkrzw::Status status = client.Set("key", "value");
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

TEST_F(ClientTest, Remove) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::RemoveRequest request;
  request.set_key("key");
  tkrzw::RemoveResponse response;
  EXPECT_CALL(*stub, Remove(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  const tkrzw::Status status = client.Remove("key");
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

TEST_F(ClientTest, Append) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::AppendRequest request;
  request.set_key("key");
  request.set_value("value");
  request.set_delim(":");
  tkrzw::AppendResponse response;
  EXPECT_CALL(*stub, Append(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  std::string value;
  const tkrzw::Status status = client.Append("key", "value", ":");
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

TEST_F(ClientTest, Increment) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::IncrementRequest request;
  request.set_key("key");
  request.set_increment(5);
  request.set_initial(100);
  tkrzw::IncrementResponse response;
  response.set_current(105);
  EXPECT_CALL(*stub, Increment(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  int64_t current = 0;
  const tkrzw::Status status = client.Increment("key", 5, &current, 100);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ(105, current);
}

TEST_F(ClientTest, Count) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::CountRequest request;
  tkrzw::CountResponse response;
  response.set_count(123);
  EXPECT_CALL(*stub, Count(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  int64_t count = 0;
  const tkrzw::Status status = client.Count(&count);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ(123, count);
}

TEST_F(ClientTest, GetFileSize) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::GetFileSizeRequest request;
  tkrzw::GetFileSizeResponse response;
  response.set_file_size(1234);
  EXPECT_CALL(*stub, GetFileSize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  int64_t file_size = 0;
  const tkrzw::Status status = client.GetFileSize(&file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ(1234, file_size);
}

TEST_F(ClientTest, Clear) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::ClearRequest request;
  tkrzw::ClearResponse response;
  EXPECT_CALL(*stub, Clear(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  const tkrzw::Status status = client.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

TEST_F(ClientTest, Rebuild) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::RebuildRequest request;
  auto* param = request.add_params();
  param->set_first("num_buckets");
  param->set_second("10");
  tkrzw::RebuildResponse response;
  EXPECT_CALL(*stub, Rebuild(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  const tkrzw::Status status = client.Rebuild({{"num_buckets", "10"}});
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

TEST_F(ClientTest, ShouldBeRebuilt) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::ShouldBeRebuiltRequest request;
  tkrzw::ShouldBeRebuiltResponse response;
  response.set_tobe(true);
  EXPECT_CALL(*stub, ShouldBeRebuilt(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  bool tobe = false;
  const tkrzw::Status status = client.ShouldBeRebuilt(&tobe);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_TRUE(tobe);
}

TEST_F(ClientTest, Synchronize) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::SynchronizeRequest request;
  request.set_hard(true);
  auto* param = request.add_params();
  param->set_first("reducer");
  param->set_second("last");
  tkrzw::SynchronizeResponse response;
  EXPECT_CALL(*stub, Synchronize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM client;
  client.InjectStub(stub.release());
  const tkrzw::Status status = client.Synchronize(true, {{"reducer", "last"}});
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
}

// END OF FILE
