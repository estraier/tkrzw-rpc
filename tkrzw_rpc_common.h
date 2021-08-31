/*************************************************************************************************
 * Common library features
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

#ifndef _TKRZW_RPC_COMMON_H
#define _TKRZW_RPC_COMMON_H

#include "tkrzw_cmd_util.h"

namespace tkrzw {

/** The string expression of the package version. */
extern const char* const RPC_PACKAGE_VERSION;

/** The string expression of the library version. */
extern const char* const RPC_LIBRARY_VERSION;

/**
 * Set the logger for global events.
 * @param logger The pointer to the logger object.  The ownership is not taken.
 */
void SetGlobalLogger(tkrzw::Logger* logger);

/**
 * Switches the process into the background.
 * @return The result status.
 */
Status DaemonizeProcess();

}  // namespace tkrzw

#endif  // _TKRZW_RPC_COMMON_H

// END OF FILE
