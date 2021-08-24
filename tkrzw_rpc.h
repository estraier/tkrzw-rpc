/*************************************************************************************************
 * RPC API of Tkrzw
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

#ifndef _TKRZW_RPC_H
#define _TKRZW_RPC_H

#include <string>
#include <string_view>
#include <tkrzw_lib_common.h>
#include <utility>
#include <vector>

namespace tkrzw {

class DBMClientImpl;

class DBMClient final {
 public:
  /**
   * Constructor.
   */
  DBMClient();

  /**
   * Destructor.
   */
  ~DBMClient();

  /**
   * Injects a stub for testing.
   * @param stub The pointer to the DBMService::StubInterface object.  The ownership is taken.
   */
  void InjectStub(void* stub);

  /**
   * Connects to the server.
   * @param host The host name of the server.
   * @param port The port number of the server.
   * @return The result status.
   */
  Status Connect(const std::string& host, int32_t port);

  /**
   * Disconnects the connection to the server.
   */
  void Disconnect();

  /**
   * Get the version numbers of the server.
   * @param key The pointer to a string object to contain the version number.
   * @return The result status.
   */
  Status GetVersion(std::string* version);

 private:
  /** Pointer to the actual implementation. */
  DBMClientImpl* impl_;
};

/**
 * Switches the process into the background.
 * @return The result status.
 */
Status DaemonizeProcess();

}  // namespace tkrzw

#endif  // _TKRZW_RPC_H

// END OF FILE
