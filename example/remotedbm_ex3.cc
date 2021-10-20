/*************************************************************************************************
 * Example for serious use cases of the remote database
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

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm_remote.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Prepare the logger.
  StreamLogger logger(&std::cout, Logger::LEVEL_INFO, " ", BaseLogger::DATE_SIMPLE_MICRO);

  // The thread to store integers in the queue.
  auto producer =
      [&]() {
        RemoteDBM dbm;
        dbm.Connect("localhost:1978").OrDie();
        dbm.Clear().OrDie();
        for (int32_t i = 1; i <= 100; i++) {
          logger.LogCat(Logger::LEVEL_INFO, "PutLast: ", i);
          dbm.PushLast(ToString(i), -1, true).OrDie();
          if (i % 10 == 0) {
            SleepThread(1);
          }
        }
        dbm.Disconnect().OrDie();
      };
  
  // The thread to emit integers from the queue.
  auto consumer =
      [&]() {
        RemoteDBM dbm;
        dbm.Connect("localhost:1978").OrDie();
        int32_t count = 0;
        while (count < 100) {
          std::string value;
          const Status status = dbm.PopFirst(nullptr, &value, 10.0);
          if (status == Status::SUCCESS) {
            logger.LogCat(Logger::LEVEL_INFO, "Got: ", value);
            count++;
          } else {
            logger.LogCat(Logger::LEVEL_ERROR, status);
          }
        }
        dbm.Disconnect().OrDie();
      };

  // Starts threads.
  std::vector<std::thread> threads;
  threads.emplace_back(std::thread(producer));
  threads.emplace_back(std::thread(consumer));

  // Joins threads.
  for (auto& thread : threads) {
    thread.join();
  }

  return 0;
}

// END OF FILE
