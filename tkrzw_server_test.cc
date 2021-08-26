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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

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

TEST_F(ServerTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  std::vector<std::unique_ptr<tkrzw::ParamDBM>> dbms(1);;
  dbms[0] = std::make_unique<tkrzw::PolyDBM>();
  const std::map<std::string, std::string> params = {{"dbm", "HashDBM"}, {"num_buckets", "10"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbms[0]->OpenAdvanced(file_path, true, tkrzw::File::OPEN_DEFAULT, params));
  tkrzw::StreamLogger logger;
  tkrzw::DBMServiceImpl server(dbms, &logger);
  grpc::ServerContext context;
  {
    tkrzw::GetVersionRequest request;
    tkrzw::GetVersionResponse response;
    grpc::Status status = server.GetVersion(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(_TKSERV_PKG_VERSION, response.version());
  }
  {
    tkrzw::InspectRequest request;
    tkrzw::InspectResponse response;
    grpc::Status status = server.Inspect(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    bool ok = false;
    for (const auto& record : response.records()) {
      if (record.first() == "class" && record.second() == "HashDBM") {
        ok = true;
      }
    }
    EXPECT_TRUE(ok);
  }
  {
    tkrzw::SetRequest request;
    request.set_key("one");
    request.set_value("first");
    tkrzw::SetResponse response;
    grpc::Status status = server.Set(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw::AppendRequest request;
    request.set_key("one");
    request.set_value("1");
    request.set_delim(":");
    tkrzw::AppendResponse response;
    grpc::Status status = server.Append(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw::CountRequest request;
    tkrzw::CountResponse response;
    grpc::Status status = server.Count(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(1, response.count());
  }
  {
    tkrzw::GetFileSizeRequest request;
    tkrzw::GetFileSizeResponse response;
    grpc::Status status = server.GetFileSize(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_GT(response.file_size(), 4096);
  }
  {
    tkrzw::GetRequest request;
    request.set_key("one");
    tkrzw::GetResponse response;
    grpc::Status status = server.Get(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ("first:1", response.value());
  }
  {
    tkrzw::RemoveRequest request;
    request.set_key("one");
    tkrzw::RemoveResponse response;
    grpc::Status status = server.Remove(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_EQ(0, dbms[0]->CountSimple());
  }
  for (int32_t i = 0; i < 30; i++) {
    const std::string expr = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->Set(expr, expr));
  }
  {
    tkrzw::ShouldBeRebuiltRequest request;
    tkrzw::ShouldBeRebuiltResponse response;
    grpc::Status status = server.ShouldBeRebuilt(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    EXPECT_TRUE(response.tobe());
  }
  {
    tkrzw::RebuildRequest request;
    auto* param = request.add_params();
    param->set_first("align_pow");
    param->set_second("0");
    tkrzw::RebuildResponse response;
    grpc::Status status = server.Rebuild(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
  {
    tkrzw::RebuildRequest request;
    auto* param = request.add_params();
    param->set_first("align_pow");
    param->set_second("0");
    tkrzw::RebuildResponse response;
    grpc::Status status = server.Rebuild(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
    bool tobe = false;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbms[0]->ShouldBeRebuilt(&tobe));
    EXPECT_FALSE(tobe);
  }
  {
    tkrzw::SynchronizeRequest request;
    tkrzw::SynchronizeResponse response;
    grpc::Status status = server.Synchronize(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.status().code());
  }
}

// END OF FILE
