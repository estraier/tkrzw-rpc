/*************************************************************************************************
 * Example for basic usage of the remote database
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

#include "tkrzw_dbm_remote.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  RemoteDBM dbm;

  // Connects to the database service.
  dbm.Connect("localhost", 1978);
  
  // Stores records.
  dbm.Set("foo", "hop");
  dbm.Set("bar", "step");
  dbm.Set("baz", "jump");

  // Retrieves records.
  std::cout << dbm.GetSimple("foo", "*") << std::endl;
  std::cout << dbm.GetSimple("bar", "*") << std::endl;
  std::cout << dbm.GetSimple("baz", "*") << std::endl;
  std::cout << dbm.GetSimple("outlier", "*") << std::endl;

  // Traverses records.
  std::unique_ptr<RemoteDBM::Iterator> iter = dbm.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << key << ":" << value << std::endl;
    iter->Next();
  }
  
  // Disconnects the connection.
  dbm.Disconnect();

  return 0;
}

// END OF FILE
