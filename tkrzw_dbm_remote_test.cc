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
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::EchoRequest request;
  request.set_message("hello");
  tkrzw::EchoResponse response;
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
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::InspectRequest request;
  tkrzw::InspectResponse response;
  auto* res_record = response.add_records();
  res_record->set_first("name");
  res_record->set_second("value");
  EXPECT_CALL(*stub, Inspect(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  std::vector<std::pair<std::string, std::string>> records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Inspect(&records));
  ASSERT_EQ(1, records.size());
  EXPECT_EQ("name", records[0].first);
  EXPECT_EQ("value", records[0].second);
}

TEST_F(RemoteDBMTest, InspectServer) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::InspectRequest request;
  request.set_dbm_index(-1);
  tkrzw::InspectResponse response;
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
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::GetRequest request;
  request.set_key("key");
  tkrzw::GetResponse response;
  response.set_value("value");
  EXPECT_CALL(*stub, Get(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get("key", &value));
  EXPECT_EQ("value", value);
}

TEST_F(RemoteDBMTest, Set) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::SetRequest request;
  request.set_key("key");
  request.set_value("value");
  request.set_overwrite(true);
  tkrzw::SetResponse response;
  EXPECT_CALL(*stub, Set(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("key", "value"));
}

TEST_F(RemoteDBMTest, Remove) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::RemoveRequest request;
  request.set_key("key");
  tkrzw::RemoveResponse response;
  EXPECT_CALL(*stub, Remove(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove("key"));
}

TEST_F(RemoteDBMTest, Append) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::AppendRequest request;
  request.set_key("key");
  request.set_value("value");
  request.set_delim(":");
  tkrzw::AppendResponse response;
  EXPECT_CALL(*stub, Append(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  std::string value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Append("key", "value", ":"));
}

TEST_F(RemoteDBMTest, Increment) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::IncrementRequest request;
  request.set_key("key");
  request.set_increment(5);
  request.set_initial(100);
  tkrzw::IncrementResponse response;
  response.set_current(105);
  EXPECT_CALL(*stub, Increment(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  int64_t current = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Increment("key", 5, &current, 100));
  EXPECT_EQ(105, current);
}

TEST_F(RemoteDBMTest, Count) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::CountRequest request;
  tkrzw::CountResponse response;
  response.set_count(123);
  EXPECT_CALL(*stub, Count(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  int64_t count = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Count(&count));
  EXPECT_EQ(123, count);
}

TEST_F(RemoteDBMTest, GetFileSize) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::GetFileSizeRequest request;
  tkrzw::GetFileSizeResponse response;
  response.set_file_size(1234);
  EXPECT_CALL(*stub, GetFileSize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  int64_t file_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.GetFileSize(&file_size));
  EXPECT_EQ(1234, file_size);
}

TEST_F(RemoteDBMTest, Clear) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::ClearRequest request;
  tkrzw::ClearResponse response;
  EXPECT_CALL(*stub, Clear(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
}

TEST_F(RemoteDBMTest, Rebuild) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::RebuildRequest request;
  auto* param = request.add_params();
  param->set_first("num_buckets");
  param->set_second("10");
  tkrzw::RebuildResponse response;
  EXPECT_CALL(*stub, Rebuild(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Rebuild({{"num_buckets", "10"}}));
}

TEST_F(RemoteDBMTest, ShouldBeRebuilt) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::ShouldBeRebuiltRequest request;
  tkrzw::ShouldBeRebuiltResponse response;
  response.set_tobe(true);
  EXPECT_CALL(*stub, ShouldBeRebuilt(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  bool tobe = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.ShouldBeRebuilt(&tobe));
  EXPECT_TRUE(tobe);
}

TEST_F(RemoteDBMTest, Synchronize) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::SynchronizeRequest request;
  request.set_hard(true);
  auto* param = request.add_params();
  param->set_first("reducer");
  param->set_second("last");
  tkrzw::SynchronizeResponse response;
  EXPECT_CALL(*stub, Synchronize(_, EqualsProto(request), _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Synchronize(true, {{"reducer", "last"}}));
}

TEST_F(RemoteDBMTest, IterateMove) {
  auto stream = std::make_unique<grpc::testing::MockClientReaderWriter<
    tkrzw::IterateRequest, tkrzw::IterateResponse>>();
  tkrzw::IterateRequest request_first;
  request_first.set_operation(tkrzw::IterateRequest::OP_FIRST);
  tkrzw::IterateRequest request_last;
  request_last.set_operation(tkrzw::IterateRequest::OP_LAST);
  tkrzw::IterateRequest request_jump;
  request_jump.set_operation(tkrzw::IterateRequest::OP_JUMP);
  request_jump.set_key("jump");
  tkrzw::IterateRequest request_jump_lower;
  request_jump_lower.set_operation(tkrzw::IterateRequest::OP_JUMP_LOWER);
  request_jump_lower.set_key("jumplower");
  tkrzw::IterateRequest request_jump_lower_inc;
  request_jump_lower_inc.set_operation(tkrzw::IterateRequest::OP_JUMP_LOWER);
  request_jump_lower_inc.set_key("jumplowerinc");
  request_jump_lower_inc.set_jump_inclusive(true);
  tkrzw::IterateRequest request_jump_upper;
  request_jump_upper.set_operation(tkrzw::IterateRequest::OP_JUMP_UPPER);
  request_jump_upper.set_key("jumpupper");
  tkrzw::IterateRequest request_jump_upper_inc;
  request_jump_upper_inc.set_operation(tkrzw::IterateRequest::OP_JUMP_UPPER);
  request_jump_upper_inc.set_key("jumpupperinc");
  request_jump_upper_inc.set_jump_inclusive(true);
  tkrzw::IterateRequest request_next;
  request_next.set_operation(tkrzw::IterateRequest::OP_NEXT);
  tkrzw::IterateRequest request_previous;
  request_previous.set_operation(tkrzw::IterateRequest::OP_PREVIOUS);
  tkrzw::IterateResponse response;
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
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();  
  EXPECT_CALL(*stub, IterateRaw(_)).WillRepeatedly(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
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
    tkrzw::IterateRequest, tkrzw::IterateResponse>>();
  tkrzw::IterateRequest request_get_both;
  request_get_both.set_operation(tkrzw::IterateRequest::OP_GET);
  tkrzw::IterateRequest request_get_none;
  request_get_none.set_operation(tkrzw::IterateRequest::OP_GET);
  request_get_both.set_omit_key(true);
  request_get_both.set_omit_value(true);
  tkrzw::IterateRequest request_get_key;
  request_get_key.set_operation(tkrzw::IterateRequest::OP_GET);
  request_get_key.set_omit_value(true);
  tkrzw::IterateRequest request_get_value;
  request_get_value.set_operation(tkrzw::IterateRequest::OP_GET);
  request_get_value.set_omit_key(true);
  tkrzw::IterateRequest request_set;
  request_set.set_operation(tkrzw::IterateRequest::OP_SET);
  request_set.set_value("set");
  tkrzw::IterateRequest request_remove;
  request_remove.set_operation(tkrzw::IterateRequest::OP_REMOVE);
  tkrzw::IterateResponse response_get_both;
  response_get_both.set_key("getbothkey");
  response_get_both.set_value("getbothvalue");
  tkrzw::IterateResponse response_get_none;
  response_get_none.mutable_status()->set_code(tkrzw::Status::NOT_FOUND_ERROR);
  tkrzw::IterateResponse response_get_key;
  response_get_key.set_key("getkeykey");
  tkrzw::IterateResponse response_get_value;
  response_get_value.set_value("getvaluevalue");
  tkrzw::IterateResponse response;
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
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();  
  EXPECT_CALL(*stub, IterateRaw(_)).WillRepeatedly(Return(stream.release()));
  tkrzw::RemoteDBM dbm;
  dbm.InjectStub(stub.release());
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

// END OF FILE
