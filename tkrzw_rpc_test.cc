/*************************************************************************************************
 * Tests for tkrzw_rpc.h
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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_rpc.h"
#include "tkrzw_rpc_mock.grpc.pb.h"
#include "tkrzw_rpc.pb.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class ClientTest : public Test {};

TEST_F(ClientTest, Basic) {
  auto stub = std::make_unique<tkrzw::MockDBMServiceStub>();
  tkrzw::GetVersionResponse response;
  response.set_version("1.2.3");
  EXPECT_CALL(*stub, GetVersion(_, _, _)).WillOnce(
      DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));
  tkrzw::DBMClient client;
  client.InjectStub(stub.release());
  std::string version;
  const tkrzw::Status status = client.GetVersion(&version);
  EXPECT_EQ(tkrzw::Status::SUCCESS, status);
  EXPECT_EQ("1.2.3", version);
}